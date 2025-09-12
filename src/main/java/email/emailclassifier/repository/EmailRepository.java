package email.emailclassifier.repository;

import email.emailclassifier.entity.Email;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface EmailRepository extends JpaRepository<Email,Long> {
    List<Email> findAllByOrderByReceivedAtDesc(Pageable pageable);
}
