package email.emailclassifier.controller;

import email.emailclassifier.service.EmailFetcherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.Map;

@Controller
public class EmailController {

    private final EmailFetcherService emailFetcherService;

    @Autowired
    public EmailController(EmailFetcherService emailFetcherService) {
        this.emailFetcherService = emailFetcherService;
    }

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @GetMapping("/fetch")
    public String fetchEmails(OAuth2AuthenticationToken auth, Model model) {
        Map<String, Long> senderCounts = emailFetcherService.fetchSenderCounts(auth);
        model.addAttribute("senderCounts", senderCounts);
        return "index";
    }
}
