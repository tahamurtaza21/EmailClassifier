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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
        return fetchSenderCounts(auth, false); // Default: process all emails
    }

    /**
     * Entry point with sampling option for ultra-fast results.
     * @param useSampling if true, process only a sample of emails for speed
     */
    public Map<String, Long> fetchSenderCounts(OAuth2AuthenticationToken auth, boolean useSampling) {
        Instant start = Instant.now();

        String token = getAccessToken(auth);
        logTime("Get Access Token", start);

        Map<String, Long> counts;
        if (useSampling) {
            System.out.println("[INFO] Using ultra-fast sampling approach");
            counts = fetchSenderCountsWithSampling(token, 1000); // Sample 1000 emails
        } else {
            System.out.println("[INFO] Using aggressive parallel approach");
            counts = fetchSenderCountsAggressively(token);
        }

        logTime("Fetch and Aggregate Domains", start);

        Map<String, Long> sortedCounts = sortCounts(counts);
        logTime("Sort Counts", start);

        return sortedCounts;
    }

    /**
     * Ultra-fast sampling approach: Process only a representative sample
     */
    private Map<String, Long> fetchSenderCountsWithSampling(String token, int sampleSize) {
        Instant stepStart = Instant.now();

        Map<String, LongAdder> counts = new ConcurrentHashMap<>();

        // Take samples from different parts of the mailbox for better representation
        Flux<String> sampledIds = fetchSampledMessageIds(token, sampleSize);

        sampledIds
                .flatMap(id -> fetchFromHeaderAsync(token, id)
                                .map(json -> extractDomain(extractHeader(json, "From")))
                                .onErrorResume(e -> Mono.empty()),
                        80) // High concurrency for samples
                .filter(Objects::nonNull)
                .doOnNext(domain -> counts.computeIfAbsent(domain, k -> new LongAdder()).increment())
                .then()
                .block();

        Map<String, Long> finalCounts = counts.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().longValue()
                ));

        logStep("fetchSenderCountsWithSampling (" + sampleSize + " samples)", stepStart);
        return finalCounts;
    }

    /**
     * Fetch a representative sample of message IDs from different parts of the mailbox
     */
    private Flux<String> fetchSampledMessageIds(String token, int maxSamples) {
        return Flux.create(sink -> {
            String pageToken = null;
            int collected = 0;
            int pageCount = 0;

            do {
                try {
                    StringBuilder uri = new StringBuilder("/gmail/v1/users/me/messages?maxResults=100");
                    if (pageToken != null) {
                        uri.append("&pageToken=").append(pageToken);
                    }

                    JsonNode response = webClient.get()
                            .uri(uri.toString())
                            .header("Authorization", "Bearer " + token)
                            .retrieve()
                            .bodyToMono(JsonNode.class)
                            .timeout(Duration.ofSeconds(10))
                            .block();

                    if (response != null && response.has("messages")) {
                        JsonNode messages = response.get("messages");

                        // Sample every Nth message for better distribution
                        int step = Math.max(1, messages.size() / 20); // Take up to 20 per page
                        for (int i = 0; i < messages.size() && collected < maxSamples; i += step) {
                            sink.next(messages.get(i).get("id").asText());
                            collected++;
                        }

                        pageCount++;
                        // Skip pages to get samples from different time periods
                        if (pageCount % 3 == 0 && response.has("nextPageToken")) {
                            pageToken = response.get("nextPageToken").asText();
                        } else if (response.has("nextPageToken")) {
                            // Skip this page token to jump ahead
                            pageToken = response.get("nextPageToken").asText();
                        } else {
                            pageToken = null;
                        }
                    } else {
                        pageToken = null;
                    }
                } catch (Exception e) {
                    sink.error(e);
                    return;
                }
            } while (pageToken != null && collected < maxSamples);

            sink.complete();
        });
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
    // Message count estimation
    // ---------------------------

    private int getMessageCount(String token) {
        try {
            JsonNode response = webClient.get()
                    .uri("/gmail/v1/users/me/messages?maxResults=1")
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block();

            return response != null && response.has("resultSizeEstimate")
                    ? response.get("resultSizeEstimate").asInt()
                    : 0;
        } catch (Exception e) {
            System.err.println("Failed to get message count: " + e.getMessage());
            return 0;
        }
    }

    // ---------------------------
    // Search-based approach for large mailboxes
    // ---------------------------

    private Map<String, Long> fetchSenderCountsWithSearch(String token) {
        Instant stepStart = Instant.now();

        Map<String, Long> domainCounts = new ConcurrentHashMap<>();

        // Get top domains using search queries for common providers
        String[] commonDomains = {
                "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
                "aol.com", "icloud.com", "protonmail.com", "live.com"
        };

        // Count messages from common domains in parallel
        Flux.fromArray(commonDomains)
                .flatMap(domain -> countMessagesFromDomain(token, domain)
                        .map(count -> Map.entry(domain, count)), 8)
                .doOnNext(entry -> {
                    if (entry.getValue() > 0) {
                        domainCounts.put(entry.getKey(), entry.getValue());
                    }
                })
                .then()
                .block();

        // Sample remaining messages to find other domains
        Set<String> otherDomains = sampleRemainingDomains(token, domainCounts.keySet(), 1000);

        for (String domain : otherDomains) {
            if (!domainCounts.containsKey(domain)) {
                long count = countMessagesFromDomain(token, domain).block();
                if (count > 0) {
                    domainCounts.put(domain, count);
                }
            }
        }

        logStep("fetchSenderCountsWithSearch", stepStart);
        return domainCounts;
    }

    private Mono<Long> countMessagesFromDomain(String token, String domain) {
        String query = URLEncoder.encode("from:@" + domain, StandardCharsets.UTF_8);

        return webClient.get()
                .uri("/gmail/v1/users/me/messages?maxResults=1&q=" + query)
                .header("Authorization", "Bearer " + token)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(response -> response != null && response.has("resultSizeEstimate")
                        ? response.get("resultSizeEstimate").asLong()
                        : 0L)
                .onErrorReturn(0L);
    }

    private Set<String> sampleRemainingDomains(String token, Set<String> knownDomains, int sampleSize) {
        Set<String> foundDomains = new HashSet<>();
        String pageToken = null;
        int processed = 0;

        try {
            do {
                StringBuilder uri = new StringBuilder("/gmail/v1/users/me/messages?maxResults=100");
                if (pageToken != null) {
                    uri.append("&pageToken=").append(pageToken);
                }

                JsonNode response = webClient.get()
                        .uri(uri.toString())
                        .header("Authorization", "Bearer " + token)
                        .retrieve()
                        .bodyToMono(JsonNode.class)
                        .block();

                if (response != null && response.has("messages")) {
                    List<String> batchIds = new ArrayList<>();
                    for (JsonNode msg : response.get("messages")) {
                        batchIds.add(msg.get("id").asText());
                        processed++;
                        if (processed >= sampleSize) break;
                    }

                    Set<String> batchDomains = processSenderBatch(token, batchIds);
                    foundDomains.addAll(batchDomains);

                    if (processed >= sampleSize) break;

                    pageToken = response.has("nextPageToken") ? response.get("nextPageToken").asText() : null;
                } else {
                    break;
                }
            } while (pageToken != null && processed < sampleSize);
        } catch (Exception e) {
            System.err.println("Error sampling domains: " + e.getMessage());
        }

        // Remove already known domains
        foundDomains.removeAll(knownDomains);
        return foundDomains;
    }

    // ---------------------------
    // Batching approach for medium mailboxes
    // ---------------------------

    private Map<String, Long> fetchAndAggregateDomainsWithBatching(String token, List<String> messageIds) {
        Instant stepStart = Instant.now();

        final int BATCH_SIZE = 100;
        List<List<String>> batches = partitionList(messageIds, BATCH_SIZE);

        Map<String, LongAdder> counts = new ConcurrentHashMap<>();

        Flux.fromIterable(batches)
                .flatMap(batch -> processBatch(token, batch),
                        Math.min(5, (batches.size() / 4) + 1)) // Limit concurrent batches
                .filter(Objects::nonNull)
                .doOnNext(domain -> counts.computeIfAbsent(domain, k -> new LongAdder()).increment())
                .then()
                .block();

        Map<String, Long> finalCounts = counts.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().longValue()
                ));

        logStep("fetchAndAggregateDomainsWithBatching", stepStart);
        return finalCounts;
    }

    private Flux<String> processBatch(String token, List<String> messageIds) {
        return Flux.fromIterable(messageIds)
                .flatMap(id -> fetchFromHeaderAsync(token, id)
                                .map(json -> extractDomain(extractHeader(json, "From")))
                                .onErrorResume(e -> Mono.empty()),
                        20) // Concurrency per batch
                .filter(Objects::nonNull);
    }

    private Set<String> processSenderBatch(String token, List<String> messageIds) {
        return Flux.fromIterable(messageIds)
                .flatMap(id -> fetchFromHeaderAsync(token, id)
                                .map(json -> extractDomain(extractHeader(json, "From")))
                                .onErrorResume(e -> Mono.empty()),
                        10)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet())
                .block();
    }

    // ---------------------------
    // Optimized individual requests for small mailboxes
    // ---------------------------

    private Map<String, Long> fetchAndAggregateDomains(String token, List<String> messageIds) {
        Instant stepStart = Instant.now();

        Map<String, LongAdder> counts = new ConcurrentHashMap<>();

        Flux.fromIterable(messageIds)
                .flatMap(id -> fetchFromHeaderAsync(token, id)
                                .map(json -> extractDomain(extractHeader(json, "From")))
                                .onErrorResume(e -> {
                                    // Reduce logging overhead - only log periodically
                                    if (messageIds.indexOf(id) % 1000 == 0) {
                                        System.err.println("Failed to fetch message " + id + ": " + e.getMessage());
                                    }
                                    return Mono.empty();
                                }),
                        50 // Concurrency level
                )
                .filter(Objects::nonNull)
                .doOnNext(domain -> counts.computeIfAbsent(domain, k -> new LongAdder()).increment())
                .then()
                .block();

        Map<String, Long> finalCounts = counts.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().longValue()
                ));

        logStep("fetchAndAggregateDomains", stepStart);
        return finalCounts;
    }

    // ---------------------------
    // Gmail API calls
    // ---------------------------

    private List<String> listAllMessageIds(String token) {
        Instant stepStart = Instant.now();

        List<String> ids = new ArrayList<>();
        String pageToken = null;

        do {
            StringBuilder uri = new StringBuilder("/gmail/v1/users/me/messages?maxResults=500");
            if (pageToken != null) {
                uri.append("&pageToken=").append(pageToken);
            }

            JsonNode response = webClient.get()
                    .uri(uri.toString())
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .timeout(Duration.ofSeconds(30))
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                    .block();

            if (response != null && response.has("messages")) {
                for (JsonNode msg : response.get("messages")) {
                    ids.add(msg.get("id").asText());
                }
                pageToken = response.has("nextPageToken") ? response.get("nextPageToken").asText() : null;
            } else {
                pageToken = null;
            }
        } while (pageToken != null);

        logStep("listAllMessageIds", stepStart);
        return ids;
    }

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

    private <T> List<List<T>> partitionList(List<T> list, int size) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
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