package ug.daes.onboarding.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import ug.daes.onboarding.model.PreferedTitles;

import java.util.List;

public interface PreferedTitlesRepo extends JpaRepository<PreferedTitles,Integer> {

//    @Query(value = "select prefered_titles from prefered_titles",nativeQuery = true)
//    List<String> getPreferedTitles();


    @Query("SELECT p.preferedTitles FROM PreferedTitles p ORDER BY CASE WHEN p.preferedTitles = 'None' THEN 1 ELSE 0 END, p.preferedTitles")
    List<String> getPreferedTitles();


}
