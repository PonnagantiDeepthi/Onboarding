package ug.daes.onboarding.util;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.dto.VisitorCardRequestDTO;
import ug.daes.onboarding.model.Subscriber;
import ug.daes.onboarding.model.SubscriberOnboardingData;
import ug.daes.onboarding.model.SubscriberRaData;

public class GenerateVisitorCardWorkerThread implements Runnable{
	
	public static Subscriber subscriber;
	public static String visitorCardURL;
	public static SubscriberRaData subscriberRaData;
	public static SubscriberOnboardingData subscriberOnboardingData;
	
	public GenerateVisitorCardWorkerThread(String visitorCardURL,Subscriber subscriber,SubscriberRaData finalRaData,SubscriberOnboardingData finalOnboardingData) {
		this.visitorCardURL = visitorCardURL;
		this.subscriber = subscriber;
		this.subscriberRaData = finalRaData;
		this.subscriberOnboardingData = finalOnboardingData;
	}

	@Override
	public void run() {
		try {
			RestTemplate restTemplate = new RestTemplate();
			
			VisitorCardRequestDTO visitorCardRequestDTO = new VisitorCardRequestDTO();
			visitorCardRequestDTO.setVisitorCardNumber(subscriber.getSubscriberUid());
			visitorCardRequestDTO.setNationality(subscriberRaData.getCountryName());
			visitorCardRequestDTO.setSuid(subscriber.getSubscriberUid());
			visitorCardRequestDTO.setDateOfBirth(subscriber.getDateOfBirth().substring(0, 10));
			visitorCardRequestDTO.setSelfieUri(subscriberOnboardingData.getSelfieUri());
			visitorCardRequestDTO.setFullName(subscriber.getFullName());
			visitorCardRequestDTO.setIdDocNumber(subscriber.getIdDocNumber());
			visitorCardRequestDTO.setGender(subscriberOnboardingData.getGender());
			visitorCardRequestDTO.setSubscriberType(subscriberOnboardingData.getSubscriberType());
			
			
			HttpHeaders httpHeaders=new HttpHeaders();
			httpHeaders.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<Object> requestEntity = new HttpEntity<>(visitorCardRequestDTO, httpHeaders);
			
			restTemplate.exchange(visitorCardURL,HttpMethod.POST,requestEntity,ApiResponse.class);
			System.out.println(" visitor card generated successfully :: "+subscriber.getFullName());
		} catch (Exception e) {
			System.out.println(" Visito card generation failed ");
			e.printStackTrace();
		}
		
	}

}
