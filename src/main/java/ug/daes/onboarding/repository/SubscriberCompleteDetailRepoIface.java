package ug.daes.onboarding.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ug.daes.onboarding.model.SubscriberCertificateDetails;
import ug.daes.onboarding.model.SubscriberCompleteDetail;

import jakarta.transaction.Transactional;

import java.util.Date;
import java.util.List;

@Transactional
@Repository
public interface SubscriberCompleteDetailRepoIface extends JpaRepository<SubscriberCompleteDetail, Integer>{

//	@Query(value = "SELECT * FROM subscriber_certificates_details scd where created_date >= ?1 and  created_date <= ?2 order by created_date DESC;",nativeQuery =  true)
//	List<SubscriberCompleteDetail> getSubscriberReports(String startDate, String endDate);

	@Query("SELECT s FROM SubscriberCertificateDetails s " +
			"WHERE s.createdDate >= :startDate AND s.createdDate <= :endDate " +
			"ORDER BY s.createdDate DESC")
	List<SubscriberCertificateDetails> getSubscriberReports(@Param("startDate") Date startDate,
															@Param("endDate") Date endDate);


	// 2. Count active devices by email or mobile number
	@Query("SELECT COUNT(s) FROM SubscriberCompleteDetail s WHERE " +
			"(s.deviceStatus = ?1 AND s.emailId = ?2) OR (s.deviceStatus = ?1 AND s.mobileNumber = ?3)")
	int getActiveDeviceCountStatusByEmailAndMobileNo(String status, String email, String mobileNo);

	// 3. Get all active subscribers by status
	@Query("SELECT s FROM SubscriberCompleteDetail s WHERE s.subscriberStatus = ?1 ORDER BY s.createdDate DESC")
	List<SubscriberCompleteDetail> getAllActiveSubscribersDetails(String status);


}
