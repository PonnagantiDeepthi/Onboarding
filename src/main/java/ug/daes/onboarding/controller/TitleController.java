package ug.daes.onboarding.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.dto.TitleDto;
import ug.daes.onboarding.model.PreferedTitles;
import ug.daes.onboarding.repository.PreferedTitlesRepo;
import ug.daes.onboarding.service.iface.PreferredTitleIface;
import ug.daes.onboarding.util.AppUtil;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@CrossOrigin
public class TitleController {

    @Autowired
    PreferedTitlesRepo repository;

    @Autowired
    PreferredTitleIface preferredTitleIface;

    @GetMapping("/api/get/preferredTitle")
    public ApiResponse getPreferredTitles() {
        return preferredTitleIface.getPreferredTitles();
    }

    @PostMapping("/api/addUpdate/title")
    public ApiResponse addUpdateTitle(@RequestBody TitleDto titleDto) {
        return preferredTitleIface.addUpdateTitle(titleDto);
    }

    @PostMapping("/api/add/preferred-titles")
    public ApiResponse savePreferredTitles(@RequestBody List<String> titles) {
        try {
            List<PreferedTitles> entities = titles.stream()
                    .map(title -> {
                        PreferedTitles pt = new PreferedTitles();
                        pt.setPreferedTitles(title);
                        return pt;
                    })
                    .collect(Collectors.toList());

            List<PreferedTitles> savedTitles = repository.saveAll(entities);

            System.out.println("Preferred titles added successfully");
            return AppUtil.createApiResponse(true, "Preferred titles added successfully", savedTitles);
        } catch (Exception e) {
            System.out.println("Preferred titles added successfully");
            return AppUtil.createApiResponse(false, "Failed to save preferred titles: " + e.getMessage(), null
            );
        }
    }
}
