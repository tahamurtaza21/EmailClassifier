package email.emailclassifier.service;

import email.emailclassifier.entity.Email;
import email.emailclassifier.repository.EmailRepository;
import jakarta.annotation.PostConstruct;
import jakarta.mail.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Service
public class EmailFetcherService {

    @Value("${mail.imap.host}")
    private String host;

    @Value("${mail.imap.user}")
    private String email;

    @Value("${mail.imap.password}")
    private String password;

    private final EmailRepository emailRepository;

    @Autowired
    public EmailFetcherService(EmailRepository emailRepository) {
        this.emailRepository = emailRepository;
    }

    @PostConstruct
    public void init(){
        try{
            fetchEmails();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void fetchEmails() throws Exception{
        Properties props = new Properties();
        props.setProperty("mail.store.protocol", "imaps");

        Session session = Session.getInstance(props);
        Store store = session.getStore("imaps");

        store.connect(host, email, password);

        Folder inbox = store.getFolder("INBOX");
        inbox.open(Folder.READ_ONLY);

        Message[] messages = inbox.getMessages();

        extractEmailsAndSave(messages);

        inbox.close(false);
        store.close();
    }

    private void extractEmailsAndSave(Message[] messages) throws MessagingException, IOException {
        List<Email> emails = new ArrayList<Email>();

        for (Message message : messages) {
            String sender = message.getFrom()[0].toString();
//            String subject = message.getSubject();
//            String body = message.getContent().toString();

            Email email = Email.builder()
                    .sender(sender)
                    .build();

            emails.add(email);
        }

        emailRepository.saveAll(emails);
    }
}
