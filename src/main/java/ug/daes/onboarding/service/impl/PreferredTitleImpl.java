package ug.daes.onboarding.service.impl;

import org.hibernate.PessimisticLockException;
import org.hibernate.QueryTimeoutException;
import org.hibernate.exception.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.dto.TitleDto;
import ug.daes.onboarding.exceptions.ExceptionHandlerUtil;
import ug.daes.onboarding.model.Subscriber;
import ug.daes.onboarding.repository.PreferedTitlesRepo;
import ug.daes.onboarding.repository.SubscriberRepoIface;
import ug.daes.onboarding.service.iface.PreferredTitleIface;
import ug.daes.onboarding.util.AppUtil;

import java.util.List;


@Service
public class PreferredTitleImpl implements PreferredTitleIface {


    @Autowired
    ExceptionHandlerUtil exceptionHandlerUtil;

    @Autowired
    SubscriberRepoIface subscriberRepoIface;

    @Autowired
    PreferedTitlesRepo preferedTitlesRepol;

    @Override
    public ApiResponse getPreferredTitles() {
        try{
            List<String> preferedTitlesList = preferedTitlesRepol.getPreferedTitles();
            System.out.println(preferedTitlesList);
            return exceptionHandlerUtil.createSuccessResponse("api.response.title.fetched",preferedTitlesList);

        } catch (Exception e) {
        e.printStackTrace();
        return exceptionHandlerUtil.handleHttpException(e);
    }

    }

    @Override
    public ApiResponse addUpdateTitle(TitleDto titleDto) {
        try {
            if (titleDto.getSuid() == null || titleDto.getSuid().isEmpty()) {
                return AppUtil.createApiResponse(false, "SUID cannot be null or empty", null);
            }

            Subscriber subscriber = subscriberRepoIface.findBysubscriberUid(titleDto.getSuid());
            if (subscriber == null) {
                return AppUtil.createApiResponse(false, "Subscriber not found for given suid", null);
            }

            if(titleDto.getTitle().equals("None")){
                subscriber.setTitle("");
                subscriberRepoIface.save(subscriber);
            }else{
                subscriber.setTitle(titleDto.getTitle());
                subscriberRepoIface.save(subscriber);
            }
            return exceptionHandlerUtil.successResponse("api.response.title.updated");
        } catch (Exception e) {
            e.printStackTrace();
            return exceptionHandlerUtil.handleHttpException(e);
        }
    }
}
