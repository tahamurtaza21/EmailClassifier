package email.emailclassifier.controller;

import org.springframework.ui.Model;
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

    @GetMapping("/")
    public String index(Model model){
        return "index";
    }

    @GetMapping("/emails/counts/senders")
    public String getSenderCountsView(Model model){
        model.addAttribute("senderCounts",emailService.classifyPerSender());
        return "index";
    }
}
