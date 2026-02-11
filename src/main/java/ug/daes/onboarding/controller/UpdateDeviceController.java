package ug.daes.onboarding.controller;

import java.text.ParseException;


import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.dto.DeviceInfo;
import ug.daes.onboarding.dto.MobileOTPDto;
import ug.daes.onboarding.exceptions.ExceptionHandlerUtil;
import ug.daes.onboarding.service.iface.DeviceUpdateIface;
import ug.daes.onboarding.service.impl.SubscriberServiceImpl;
import ug.daes.onboarding.util.Utility;


@RestController
public class UpdateDeviceController {

    private static Logger logger = LoggerFactory.getLogger(SubscriberServiceImpl.class);

    /** The Constant CLASS. */
    final static String CLASS = "UpdateDeviceController";

    @Autowired
    DeviceUpdateIface deviceUpdateIface;
    
    @Autowired
    MessageSource messageSource;
    
    @Autowired
    ExceptionHandlerUtil exceptionHandlerUtil;
    

    @PostMapping("/api/post/verify-new-device")
    public ApiResponse verifyNewDevice(HttpServletRequest request, @RequestBody MobileOTPDto subscriberDTO) throws ParseException {
    	logger.info("{} - {} - verify new device req for {}", CLASS, Utility.getMethodName(), subscriberDTO);
        logger.info("{} - {} - verifySubscriberDetails req for deviceId, appVersion, osVersion: {}, {}, {}", CLASS, Utility.getMethodName(), request.getHeader("deviceId"), request.getHeader("appVersion"), request.getHeader("osVersion"));
        DeviceInfo deviceInfoObj = new DeviceInfo(request.getHeader("deviceId"), request.getHeader("appVersion"), request.getHeader("osVersion"));
        if (deviceInfoObj.getDeviceId() == null || deviceInfoObj.getOsVersion() == null || deviceInfoObj.getAppVersion() == null) {
        	return exceptionHandlerUtil.createErrorResponse("api.error.one.or.moredevice.info.is.missing");
        } 
        subscriberDTO.setSubscriberEmail(subscriberDTO.getSubscriberEmail().toLowerCase());
        return deviceUpdateIface.validateSubscriberAndDevice(deviceInfoObj, subscriberDTO);
    }


    @PostMapping("/api/post/activate-new-device")
    public ApiResponse verifySubscriberDetails(HttpServletRequest request, @RequestBody MobileOTPDto mobileOTPDto){
        try{
        	logger.info("{} - {} - verify new device req for {}", CLASS, Utility.getMethodName(), mobileOTPDto);
            logger.info("{} - {} - verifySubscriberDetails req for deviceId, appVersion, osVersion: {}, {}, {}", CLASS, Utility.getMethodName(), request.getHeader("deviceId"), request.getHeader("appVersion"), request.getHeader("osVersion"));
            DeviceInfo deviceInfoObj = new DeviceInfo(request.getHeader("deviceId"), request.getHeader("appVersion"), request.getHeader("osVersion"));
            
            if(deviceInfoObj.getDeviceId()==null || deviceInfoObj.getOsVersion()==null || deviceInfoObj.getAppVersion()==null) {
            	return exceptionHandlerUtil.createErrorResponse("api.error.one.or.moredevice.info.is.missing");
            }
            return deviceUpdateIface.activateNewDevice(deviceInfoObj,mobileOTPDto);
        }catch (Exception e){
            e.printStackTrace();
            return ExceptionHandlerUtil.handleException(e);
        }

    }
}
