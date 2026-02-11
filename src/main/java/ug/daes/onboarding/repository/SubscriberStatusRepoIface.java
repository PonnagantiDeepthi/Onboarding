/**
 * 
 */
package ug.daes.onboarding.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import ug.daes.onboarding.model.SubscriberStatus;

/**
 * @author Raxit Dubey
 *
 */
@Repository
public interface SubscriberStatusRepoIface extends JpaRepository<SubscriberStatus, Integer>{


	@Query("SELECT s FROM SubscriberStatus s WHERE s.subscriberUid = :suid")
	SubscriberStatus findBysubscriberUid(@Param("suid") String suid);
	
}
