package email.emailclassifier.Controller;

import email.emailclassifier.service.EmailService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.Map;

@Controller
public class EmailController {

    private final EmailService emailService;

    @Autowired
    public EmailController(EmailService emailService) {
        this.emailService = emailService;
    }

    @GetMapping("/counts/sender")
    public Map<String,Long> getSenderCounts(){
        return emailService.classifyPerSender();
    }
}
