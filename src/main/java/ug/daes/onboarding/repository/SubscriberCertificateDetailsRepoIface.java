package ug.daes.onboarding.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import ug.daes.onboarding.model.SubscriberCertificateDetails;

@Repository
public interface SubscriberCertificateDetailsRepoIface extends JpaRepository<SubscriberCertificateDetails, String>{
//
//	@Query(value = "SELECT * FROM subscriber_certificates_details scd where created_date >= date(?1) and  created_date <= date(?2) order by created_date DESC;",nativeQuery =  true)
//	List<SubscriberCertificateDetails> getSubscriberReports(String startDate, String endDate);


//	@Query(
//			value = "SELECT * FROM subscriber_certificates_details scd " +
//					"WHERE CAST(scd.created_date AS DATE) >= CAST(?1 AS DATE) " +
//					"AND   CAST(scd.created_date AS DATE) <= CAST(?2 AS DATE) " +
//					"ORDER BY CAST(scd.created_date AS DATE) DESC",
//			nativeQuery = true
//	)
//	List<SubscriberCertificateDetails> getSubscriberReports(String startDate, String endDate);

	@Query("SELECT scd FROM SubscriberCertificateDetails scd " +
			"WHERE FUNCTION('TO_DATE', scd.createdDate, 'YYYY-MM-DD') >= FUNCTION('TO_DATE', :startDate, 'YYYY-MM-DD') " +
			"AND   FUNCTION('TO_DATE', scd.createdDate, 'YYYY-MM-DD') <= FUNCTION('TO_DATE', :endDate, 'YYYY-MM-DD') " +
			"ORDER BY FUNCTION('TO_DATE', scd.createdDate, 'YYYY-MM-DD') DESC")
	List<SubscriberCertificateDetails> getSubscriberReports(@Param("startDate") String startDate,
															@Param("endDate") String endDate);


//	@Query("SELECT scd FROM SubscriberCertificateDetails scd WHERE scd.createdDate >= :startDate AND scd.createdDate <= :endDate ORDER BY scd.createdDate DESC")
//	List<SubscriberCertificateDetails> getSubscriberReports(java.util.Date startDate, java.util.Date endDate);
//
//	List<SubscriberCertificateDetails> getSubscriberReports(String startDate, String endDate);
}
