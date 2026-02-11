package ug.daes.onboarding.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import ug.daes.onboarding.model.ConsentHistory;

import org.springframework.data.domain.Pageable;

import java.util.List;


@Repository
public interface ConsentHistoryRepo extends JpaRepository<ConsentHistory, Integer> {

//    @Query("SELECT c FROM ConsentHistory c WHERE c.consentRequired = true ORDER BY c.createdOn DESC")
//    ConsentHistory findLatestConsent();

//    @Query("SELECT c FROM ConsentHistory c WHERE c.consentRequired = true ORDER BY c.createdOn DESC")
//    List<ConsentHistory> findLatestConsent();


    @Query("SELECT c FROM ConsentHistory c WHERE c.consentRequired = true ORDER BY c.createdOn DESC")
    List<ConsentHistory> findLatestConsent();


    ConsentHistory findTopByConsentIdOrderByCreatedOnDesc(int id);



//    @Query("SELECT c FROM ConsentHistory c WHERE c.consentId = ?1 ORDER BY c.createdOn DESC")
//    ConsentHistory LatestConsent(int id);
}
