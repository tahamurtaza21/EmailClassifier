package email.emailclassifier.service;

import email.emailclassifier.entity.Email;
import email.emailclassifier.repository.EmailRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class EmailService {

    private final EmailRepository emailRepository;

    public EmailService(EmailRepository emailRepository) {
        this.emailRepository = emailRepository;
    }

    public Map<String, Long> classifyPerSender() {
        List<Email> last1000 = emailRepository.findAllByOrderByReceivedAtDesc();

        return last1000.stream()
                .collect(Collectors.groupingBy(
                        Email::getSender,
                        Collectors.counting()
                ))
                .entrySet()
                .stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed()) // sort by count desc
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new // preserve order
                ));
    }


}
