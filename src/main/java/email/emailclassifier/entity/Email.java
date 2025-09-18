package email.emailclassifier.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Email {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String sender;
    private String subject;

    @Lob
    @Column(columnDefinition = "CLOB")
    private String body;

    private String classification;
    private LocalDateTime receivedAt;
}
