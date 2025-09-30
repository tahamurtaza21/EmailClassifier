package email.emailclassifier.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.mail.internet.InternetAddress;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.retry.Retry;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

@Service
public class EmailFetcherService {

    private final OAuth2AuthorizedClientService authorizedClientService;
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final Map<String, String> domainCache = new ConcurrentHashMap<>();

    public EmailFetcherService(OAuth2AuthorizedClientService authorizedClientService,
                               WebClient.Builder webClientBuilder) {
        this.authorizedClientService = authorizedClientService;
        this.objectMapper = new ObjectMapper();

        // Configure aggressive connection pooling for maximum speed
        ConnectionProvider connectionProvider = ConnectionProvider.builder("gmail-pool")
                .maxConnections(200) // Increased connections
                .maxIdleTime(Duration.ofSeconds(10)) // Shorter idle time
                .maxLifeTime(Duration.ofSeconds(30)) // Shorter lifetime
                .pendingAcquireTimeout(Duration.ofSeconds(30))
                .evictInBackground(Duration.ofSeconds(60))
                .build();

        HttpClient httpClient = HttpClient.create(connectionProvider)
                .responseTimeout(Duration.ofSeconds(15)) // Shorter timeout
                .keepAlive(true);

        this.webClient = webClientBuilder
                .baseUrl("https://gmail.googleapis.com")
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }

    /**
     * Entry point: fetch sender counts with aggressive optimizations for medium mailboxes.
     * Includes optional sampling for ultra-fast results.
     */
    public Map<String, Long> fetchSenderCounts(OAuth2AuthenticationToken auth) {
        Instant start = Instant.now();

        String token = getAccessToken(auth);
        logTime("Get Access Token", start);

        Map<String, Long> counts;
        System.out.println("[INFO] Using aggressive parallel approach");
        counts = fetchSenderCountsAggressively(token);

        Map<String, Long> sortedCounts = sortCounts(counts);
        logTime("Sort Counts", start);

        return sortedCounts;
    }

    /**
     * Aggressive approach: Fetch message IDs and domains in parallel streams
     */
    private Map<String, Long> fetchSenderCountsAggressively(String token) {
        Instant stepStart = Instant.now();

        // Use higher concurrency and parallel processing
        Map<String, LongAdder> counts = new ConcurrentHashMap<>();

        // Fetch all message IDs with higher concurrency
        Flux<String> messageIdStream = fetchAllMessageIdsStream(token);

        // Process domains with very high concurrency
        messageIdStream
                .flatMap(id -> fetchFromHeaderAsync(token, id)
                                .map(json -> extractDomain(extractHeader(json, "From")))
                                .onErrorResume(e -> Mono.empty()),
                        100) // Very high concurrency for medium mailboxes
                .filter(Objects::nonNull)
                .doOnNext(domain -> counts.computeIfAbsent(domain, k -> new LongAdder()).increment())
                .then()
                .block();

        Map<String, Long> finalCounts = counts.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().longValue()
                ));

        logStep("fetchSenderCountsAggressively", stepStart);
        return finalCounts;
    }

    /**
     * Stream all message IDs without collecting them into a list first
     */
    private Flux<String> fetchAllMessageIdsStream(String token) {
        return Flux.create(sink -> {
            String pageToken = null;

            do {
                try {
                    StringBuilder uri = new StringBuilder("/gmail/v1/users/me/messages?maxResults=500");
                    if (pageToken != null) {
                        uri.append("&pageToken=").append(pageToken);
                    }

                    JsonNode response = webClient.get()
                            .uri(uri.toString())
                            .header("Authorization", "Bearer " + token)
                            .retrieve()
                            .bodyToMono(JsonNode.class)
                            .timeout(Duration.ofSeconds(15))
                            .block();

                    if (response != null && response.has("messages")) {
                        for (JsonNode msg : response.get("messages")) {
                            sink.next(msg.get("id").asText());
                        }
                        pageToken = response.has("nextPageToken") ? response.get("nextPageToken").asText() : null;
                    } else {
                        pageToken = null;
                    }
                } catch (Exception e) {
                    sink.error(e);
                    return;
                }
            } while (pageToken != null);

            sink.complete();
        });
    }

    // ---------------------------
    // Token handling
    // ---------------------------

    private String getAccessToken(OAuth2AuthenticationToken auth) {
        OAuth2AuthorizedClient client = authorizedClientService.loadAuthorizedClient(
                auth.getAuthorizedClientRegistrationId(),
                auth.getName());

        if (client == null) {
            throw new IllegalStateException("No OAuth2 client found for user");
        }
        return client.getAccessToken().getTokenValue();
    }

    // ---------------------------
    // Gmail API calls
    // ---------------------------

    private Mono<JsonNode> fetchFromHeaderAsync(String token, String messageId) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder.path("/gmail/v1/users/me/messages/{id}")
                        .queryParam("format", "metadata")
                        .queryParam("metadataHeaders", "From")
                        .build(messageId))
                .header("Authorization", "Bearer " + token)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .timeout(Duration.ofSeconds(5)) // Shorter timeout for faster failure
                .retry(1); // Reduced retries for speed
    }

    // ---------------------------
    // Processing and utility functions
    // ---------------------------

    private Map<String, Long> sortCounts(Map<String, Long> counts) {
        Instant stepStart = Instant.now();

        Map<String, Long> sorted = counts.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));

        logStep("sortCounts", stepStart);
        return sorted;
    }

    private String extractDomain(String fromHeader) {
        if (fromHeader == null) return null;

        // Check cache first
        String cached = domainCache.get(fromHeader);
        if (cached != null) return cached;

        try {
            InternetAddress ia = new InternetAddress(fromHeader);
            String emailAddr = ia.getAddress();
            if (emailAddr != null && emailAddr.contains("@")) {
                String domain = emailAddr.substring(emailAddr.indexOf('@') + 1).toLowerCase();
                // Cache the result
                if (domainCache.size() < 10000) { // Prevent unbounded cache growth
                    domainCache.put(fromHeader, domain);
                }
                return domain;
            }
        } catch (jakarta.mail.internet.AddressException ignored) {
        }

        String fallback = fromHeader.toLowerCase();
        if (domainCache.size() < 10000) {
            domainCache.put(fromHeader, fallback);
        }
        return fallback;
    }

    private String extractHeader(JsonNode message, String name) {
        JsonNode headers = message.path("payload").path("headers");
        if (headers.isArray()) {
            for (JsonNode header : headers) {
                if (header.get("name").asText().equalsIgnoreCase(name)) {
                    return header.get("value").asText();
                }
            }
        }
        return null;
    }

    // ---------------------------
    // Performance logging
    // ---------------------------

    private void logStep(String stepName, Instant stepStart) {
        long ms = Duration.between(stepStart, Instant.now()).toMillis();
        System.out.println("[PERF] " + stepName + " took " + ms + " ms");
    }

    private void logTime(String label, Instant start) {
        long ms = Duration.between(start, Instant.now()).toMillis();
        System.out.println("[PERF] " + label + " (elapsed: " + ms + " ms)");
    }
}