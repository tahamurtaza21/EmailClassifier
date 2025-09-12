package email.emailclassifier.service;

import email.emailclassifier.entity.Email;
import email.emailclassifier.repository.EmailRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class EmailService {

    private final EmailRepository emailRepository;
    private final ClassifierService classifierService;;

    public EmailService(EmailRepository emailRepository, ClassifierService classifierService) {
        this.emailRepository = emailRepository;
        this.classifierService = classifierService;
    }

    public Map<String,Long> classifyPerSender(){
        List<Email> last1000 = emailRepository.findAllByOrderByReceivedAtDesc(PageRequest.of(0, 1000));

        return last1000.stream().collect(Collectors.groupingBy(
                Email::getSender,
                Collectors.counting())
        );
    }
}
