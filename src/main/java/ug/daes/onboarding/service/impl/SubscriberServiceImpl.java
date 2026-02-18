
package ug.daes.onboarding.service.impl;

import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;

import org.hibernate.PessimisticLockException;
import org.hibernate.QueryTimeoutException;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.DataException;
import org.hibernate.exception.GenericJDBCException;
import org.hibernate.exception.JDBCConnectionException;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.SQLGrammarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import ug.daes.DAESService;
import ug.daes.PKICoreServiceException;
import ug.daes.Result;
import ug.daes.onboarding.config.SentryClientExceptions;
import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.constant.Constant;
import ug.daes.onboarding.dto.*;
import ug.daes.onboarding.enums.LogMessageType;
import ug.daes.onboarding.enums.ServiceNames;
import ug.daes.onboarding.enums.TransactionType;
import ug.daes.onboarding.exceptions.ExceptionHandlerUtil;
import ug.daes.onboarding.exceptions.OnBoardingServiceException;
import ug.daes.onboarding.model.*;
import ug.daes.onboarding.repository.*;
import ug.daes.onboarding.service.iface.DeviceUpdateIface;
import ug.daes.onboarding.service.iface.SubscriberServiceIface;
import ug.daes.onboarding.service.iface.TemplateServiceIface;
import ug.daes.onboarding.util.*;

import static ug.daes.onboarding.util.VersionComparatorThread.subscriber;

@Primary
@Service
public class SubscriberServiceImpl implements SubscriberServiceIface {

	private static Logger logger = LoggerFactory.getLogger(SubscriberServiceImpl.class);

	/**
	 * The Constant CLASS.
	 */
	final static String CLASS = "SubscriberServiceImpl";

	@Autowired
	SubscriberRepoIface subscriberRepoIface;

	@Autowired
	SubscriberDeviceRepoIface deviceRepoIface;

	@Autowired
	SubscriberDeviceHistoryRepoIface subscriberDeviceHistoryRepoIface;

	@Autowired
	SubscriberFcmTokenRepoIface fcmTokenRepoIface;

	@Autowired
	SubscriberOnboardingDataRepoIface onboardingDataRepoIface;

	@Autowired
	SubscriberStatusRepoIface statusRepoIface;

	@Autowired
	SubscriberRaDataRepoIface raRepoIface;

	@Autowired
	OnBoardingTemplateRepoIface onBoardingTemplateRepoIface;

	@Autowired
	SubscriberCertificatesRepoIface subscriberCertificatesRepoIface;

	@Autowired
	SubscriberCertPinHistoryRepoIface subscriberCertPinHistoryRepoIface;

	@Autowired
	OnboardingLivelinessRepository livelinessRepository;

	@Autowired
	TemplateServiceIface templateServiceIface;

	@Autowired
	MapMethodObStepRepoIface mapStepRepoIface;

	@Autowired
	TrustedUserRepoIface trustedUserRepoIface;

	@Autowired
	SubscriberDeletionRepository subscriberDeletionRepository;

	@Autowired
	SubscriberCertificateDetailsRepoIface subscriberCertificateDetailsRepoIface;

	@Autowired
	SubscriberCompleteDetailRepoIface subscriberCompleteDetailRepoIface;

	@Autowired
	KafkaSender mqSender;

	@Autowired
	EdmsServiceImpl edmsService;

	@Autowired
	public RestTemplate restTemplate;

	@Autowired
	SubscriberViewRepoIface subscriberViewRepoIface;

	@Autowired
	SusbcriberDetailsViewRepo susbcriberDetailsRepository;
	@Autowired
	SubscriberConsentsRepo subscriberConsentsRepo;

	@Autowired
	ConsentHistoryRepo consentHistoryRepo;

	ObjectMapper objectMapper = new ObjectMapper();

	@Autowired
	SentryClientExceptions sentryClientExceptions;

	@Autowired
	MinioStorageServiceImpl minioStorageService;

	@Value("${is.onboarding.fee}")
	boolean isOnboardingFee;

	@Value("${dtportal.base.url}")
	private String dtportal;

	@Value("${ra.priauth.scheme}")
	private String priAuthScheme;

	@Value("${priauth.url.boolean}")
	private boolean priAuthSchemeBoolean;

	@Value(value = "${email.url}")
	private String emailBaseUrl;
	@Value(value = "${ind.api.sms}")
	private String indApiSMS;
	@Value(value = "${nira.api.sms}")
	private String niraApiSMS;

	@Value(value = "${nira.username}")
	private String niraUserName;

	@Value(value = "${nira.password}")
	private String niraPassword;
	@Value(value = "${nira.api.token}")
	private String niraApiToken;

	@Value(value = "${uae.api.sms}")
	private String uaeApiSMS;

	@Value(value = "${registerface.url}")
	private String registerFaceURL;

	@Value("${register.face.url}")
	private boolean registerFaceBoolean;

	@Value("${test.android.email}")
	private String testAndroidEmail;

	@Value("${test.ios.email}")
	private String testIosEmail;

	@Value("${test.android.mobile.no}")
	private String testAndroidOtp;

	@Value("${test.ios.mobile.no}")
	private String testIosOtp;

	@Value("${border.control.photo.isrequired}")
	private boolean boarderControllPhotoRequired;
	@Value(value = "${remove.background.url}")
	private String removeBackGroundFromImageURL;

	@Value(value = "${remove.background.boolean}")
	private boolean removeBackGroundFromImageBoolean;

//	@Value("${edms.password}")
//	private String password;
//
//	@Value("${edms.baseurl}")
//	private String baseUrl;

	@Value("${ra.base.url}")
	private String raBaseUrl;

	@Value(value = "${nira.api.timetolive}")
	private int timeToLive;

	@Value("${au.log.url}")
	private String auditLogUrl;

	@Value("${config.validation.allowTrustedUsersOnly}")
	private String trustedUserStatus;

	// Re-onboard
	@Value("${re.onboard.dateofbirth}")
	private boolean checkDateOfBirth;

	@Value("${re.onboard.gender}")
	private boolean checkGender;

	@Value("${re.onboard.documentnumber}")
	private boolean checkDocumentNumber;

	@Value("${expiry.days}")
	private int expiryDays;

	@Value("${signed.required.by.user}")
	private boolean signRequired;

	@Value("${visitorCardUrl}")
	private String visitorCardUrl;

	@Autowired
	LogModelServiceImpl logModelServiceImpl;

	@Autowired
	SubscriberHistoryRepo subscriberHistoryRepo;

	@Autowired
	OnBoardingMethodRepoIface onBoardingMethodRepoIface;

	@Autowired
	OrgContactsEmailRepository orgContactsEmailRepository;

	@Autowired
	DeviceUpdateIface deviceUpdateIface;

//	@Autowired
//	SentryClientExceptions sentryClientExceptions;

	@Autowired
	OnBoardingServiceException onBoardingServiceException;

	@Autowired
	ExceptionHandlerUtil exceptionHandlerUtil;

	@Autowired
	SubscriberPersonalDocumentRepo subscriberPersonalDocumentRepo;

	public String generateSubscriberUniqueId() {
		UUID uuid = UUID.randomUUID();
		logger.info(CLASS + "Generate Subscriber UniqueId {}", uuid.toString());
		return uuid.toString();
	}

	private String encryptedString(String s) {
		try {
			// System.out.println("s => " + s);
			Result result = DAESService.encryptData(s);
			return new String(result.getResponse());
		} catch (Exception e) {
			e.printStackTrace();
			return e.getMessage();
		}
	}

	@SuppressWarnings("unused")
	@Override
	public ApiResponse saveSubscribersData(MobileOTPDto subscriberDTO) throws ParseException, UnknownHostException {

		String result = ValidationUtil.validate(subscriberDTO);
		if (result != null) {
			System.out.println(" saveSubscribersData Validation errors: " + result);
			return exceptionHandlerUtil.createFailedResponseWithCustomMessage(result, null);
		}
		if (subscriberDTO.getOsName() == null || subscriberDTO.getAppVersion() == null
				|| subscriberDTO.getOsVersion() == null || subscriberDTO.getDeviceInfo() == null) {
			return exceptionHandlerUtil.createErrorResponse("api.error.application.info.not.found");
		} else if (subscriberDTO.getFcmToken() == null && subscriberDTO.getFcmToken().isEmpty()) {
			return exceptionHandlerUtil.createErrorResponse("api.error.fcmtoken.cant.be.null.or.empty");
		}
		Date startTime = new Date();
		String OtpReqTime = AppUtil.getTimeStamping();
		Subscriber subscriber = new Subscriber();
		SubscriberDevice subscriberDevice = new SubscriberDevice();
		SubscriberFcmToken fcmToken = new SubscriberFcmToken();
		SubscriberStatus subscriberStatus = new SubscriberStatus();

		SubscriberRegisterResponseDTO responseDTO = new SubscriberRegisterResponseDTO();

		if (!subscriberDTO.getOtpStatus()) {
			return exceptionHandlerUtil.createErrorResponse("api.error.otp.not.verified");
		}
		ApiResponse response = checkValidationForSubscriber(subscriberDTO);
		logger.info("{}{} - Response from checkValidationForSubscriber in saveSubscriberData: {}", CLASS,
				Utility.getMethodName(), response);
		if (!response.isSuccess() && response.getResult() != null) {
			response.setSuccess(true);
			return response;
		}
		if (!response.isSuccess() && response.getResult() == null) {
			return response;
		}

		try {
			String suid = generateSubscriberUniqueId();
			logger.info(CLASS + "saveSubscriberData req for suid {}", suid);
			if (subscriberDTO != null) {
				Subscriber previousSuid = subscriberRepoIface.getSubscriberUidByEmailAndMobile(
						subscriberDTO.getSubscriberEmail(), subscriberDTO.getSubscriberMobileNumber());
				if (previousSuid != null) {
					SubscriberFcmToken preSubscriberFcmToken = fcmTokenRepoIface
							.findBysubscriberUid(previousSuid.getSubscriberUid());
					SubscriberStatus preSubscriberStatus = statusRepoIface
							.findBysubscriberUid(previousSuid.getSubscriberUid());
					SubscriberDevice preSubscriberDevice = deviceRepoIface
							.getSubscriber(previousSuid.getSubscriberUid());

					if (preSubscriberDevice.getDeviceStatus().equals(Constant.DEVICE_STATUS_DISABLED)) {

						SubscriberDeviceHistory subscriberDeviceHistory = new SubscriberDeviceHistory();
						subscriberDeviceHistory.setSubscriberUid(previousSuid.getSubscriberUid());
						subscriberDeviceHistory.setDeviceUid(preSubscriberDevice.getDeviceUid());
						subscriberDeviceHistory.setDeviceStatus(Constant.DEVICE_STATUS_DISABLED);
						subscriberDeviceHistory.setCreatedDate(AppUtil.getDate());
						subscriberDeviceHistory.setUpdatedDate(AppUtil.getDate());
						subscriberDeviceHistoryRepoIface.save(subscriberDeviceHistory);

					}

					System.out.println("previousSuid :: " + previousSuid);
					subscriber.setSubscriberId(previousSuid.getSubscriberId());
					subscriber.setSubscriberUid(previousSuid.getSubscriberUid());

					subscriberDevice.setSubscriberDeviceId(preSubscriberDevice.getSubscriberDeviceId());
					subscriberDevice.setSubscriberUid(previousSuid.getSubscriberUid());

					fcmToken.setSubscriberFcmTokenId(preSubscriberFcmToken.getSubscriberFcmTokenId());
					fcmToken.setSubscriberUid(previousSuid.getSubscriberUid());
					subscriberStatus.setSubscriberStatusId(preSubscriberStatus.getSubscriberStatusId());
					subscriberStatus.setSubscriberUid(previousSuid.getSubscriberUid());
					responseDTO.setSuID(previousSuid.getSubscriberUid());

				} else {
					subscriber.setSubscriberUid(suid);
					subscriberDevice.setSubscriberUid(suid);
					fcmToken.setSubscriberUid(suid);
					subscriberStatus.setSubscriberUid(suid);
					responseDTO.setSuID(suid);
				}

				subscriber.setCreatedDate(AppUtil.getDate());
				subscriber.setUpdatedDate(AppUtil.getDate());
				subscriber.setEmailId(subscriberDTO.getSubscriberEmail().toLowerCase());
				subscriber.setMobileNumber(subscriberDTO.getSubscriberMobileNumber());
				subscriber.setFullName(subscriberDTO.getSubscriberName());
				subscriber.setOsName(subscriberDTO.getOsName());
				subscriber.setOsVersion(subscriberDTO.getOsVersion());
				subscriber.setDeviceInfo(subscriberDTO.getDeviceInfo());
				subscriber.setAppVersion(subscriberDTO.getAppVersion());

				// subscriber.setSmartPhoneUser(true);

				subscriberDevice.setCreatedDate(AppUtil.getDate());
				subscriberDevice.setUpdatedDate(AppUtil.getDate());
				subscriberDevice.setDeviceUid(subscriberDTO.getDeviceId());
				subscriberDevice.setDeviceStatus(Constant.DEVICE_STATUS_ACTIVE);

				fcmToken.setCreatedDate(AppUtil.getDate());
				fcmToken.setFcmToken(subscriberDTO.getFcmToken());

				subscriberStatus.setOtpVerifiedStatus(Constant.OTP_VERIFIED_STATUS);
				subscriberStatus.setSubscriberStatus(Constant.SUBSCRIBER_STATUS);
				subscriberStatus.setCreatedDate(AppUtil.getDate());
				subscriberStatus.setUpdatedDate(AppUtil.getDate());

				subscriber = subscriberRepoIface.save(subscriber);

				if (previousSuid != null) {
//					deviceRepoIface.insertSubscriber(previousSuid.getSubscriberId(),previousSuid.getSubscriberUid(), subscriberDTO.getDeviceId(),
//							"ACTIVE", AppUtil.getDate(), AppUtil.getDate());

					SubscriberDevice device = deviceRepoIface.getSubscriber(previousSuid.getSubscriberUid());

					System.out.println("old device  >> " + device.getSubscriberDeviceId());
					deviceRepoIface.updateSubscriber(subscriberDTO.getDeviceId(), "ACTIVE", AppUtil.getDate(),
							device.getSubscriberDeviceId());

					System.out.println("Old device updated with new deviceid and Status ");

				} else {
					subscriberDevice = deviceRepoIface.save(subscriberDevice);
				}
//				if (previousSuid != null) {
//					deviceRepoIface.insertSubscriber(previousSuid.getSubscriberUid(), subscriberDTO.getDeviceId(),
//							"ACTIVE", AppUtil.getDate(), AppUtil.getDate());
//				} else {
//					subscriberDevice = deviceRepoIface.save(subscriberDevice);
//				}

//				if (previousSuid != null) {
//					String firstTimeOnboarding = subscriberRepoIface
//							.firstTimeOnboardingPaymentStatus(previousSuid.getSubscriberUid());
//					if (firstTimeOnboarding != null) {
//						responseDTO.setFirstTimeOnboarding(false);
//					} else {
//						responseDTO.setFirstTimeOnboarding(true);
//					}
//				}
//
				if (previousSuid != null) {
					List<String> firstTimeOnboardingList = subscriberRepoIface
							.firstTimeOnboardingPaymentStatus(previousSuid.getSubscriberUid());

					String firstTimeOnboarding = firstTimeOnboardingList.isEmpty() ? null
							: firstTimeOnboardingList.get(0);

					if (firstTimeOnboarding != null) {
						responseDTO.setFirstTimeOnboarding(false);
					} else {
						responseDTO.setFirstTimeOnboarding(true);
					}
				} else {
					responseDTO.setFirstTimeOnboarding(true);
				}

				fcmToken = fcmTokenRepoIface.save(fcmToken);
				subscriberStatus = statusRepoIface.save(subscriberStatus);

				if (subscriber != null) {

					responseDTO.setSubscriberStatus(Constant.SUBSCRIBER_STATUS);
					Date endTime = new Date();

					double toatlTime = AppUtil.getDifferenceInSeconds(startTime, endTime);
					System.out.println("toatlTime :: " + toatlTime);

					logModelServiceImpl.setLogModel(true, subscriber.getSubscriberUid(), null,
							"SUBSCRIBER_REGISTRATION", subscriber.getSubscriberUid(), String.valueOf(toatlTime),
							startTime, endTime, null);
					logger.info(CLASS + " saveSubscriberData Subscriber Detail saved {}", responseDTO);
					if (!signRequired) {
						SubscriberConsents subscriberConsents = new SubscriberConsents();
						String consentData = "I agreed to above Terms and conditions and Data privacy terms";
						List<ConsentHistory> latestConsentList = consentHistoryRepo.findLatestConsent();

						ConsentHistory consentHistory = latestConsentList.isEmpty() ? null : latestConsentList.get(0);

						if (consentHistory == null) {
							// No consent history found, handle gracefully

							// maybe skip consent validation or return early
						} else {
							// Safe to call getId()
							if (subscriberConsentsRepo.findSubscriberConsentBySuidAndConsentId(responseDTO.getSuID(),
									consentHistory.getId()) == null) {

								subscriberConsents.setCreatedOn(AppUtil.getDate());
								subscriberConsents.setConsentData(consentData);
								subscriberConsents.setSuid(responseDTO.getSuID());
								subscriberConsents.setConsentId(consentHistory.getId());
								subscriberConsentsRepo.save(subscriberConsents);

								// proceed with logic when subscriber has not given consent
							}
						}

					}
					return exceptionHandlerUtil.createSuccessResponse(
							"api.response.subscriber.email.and.mobile.number.is.verified", responseDTO);
//					return AppUtil.createApiResponse(true,
//							"api.response.subscriber.email.and.mobile.number.is.verified",
//									null, Locale.ENGLISH),
//							responseDTO);
				} else {
					logModelServiceImpl.setLogModel(false, subscriber.getSubscriberUid(), null,
							"SUBSCRIBER_REGISTRATION", subscriber.getSubscriberUid(), null, null, null, null);

					return exceptionHandlerUtil
							.createErrorResponse("api.response.subscriber.email.and.mobile.number.is.not.verified");
//					return AppUtil.createApiResponse(false,
//							"api.response.subscriber.email.and.mobile.number.is.not.verified",
//									null, Locale.ENGLISH),
//							null);
				}
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.empty.fields");
//				return AppUtil.createApiResponse(false,
//						"api.error.empty.fields", null);
			}

		} catch (Exception e) {
			e.printStackTrace();
			sentryClientExceptions.captureTags(subscriberDTO.getSuID(), subscriberDTO.getSubscriberMobileNumber(),
					"saveSubscribersData", "SubscriberController");
			sentryClientExceptions.captureExceptions(e);
			logger.error(CLASS + "saveSubscriberData Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);
//			return AppUtil.createApiResponse(false,
//					"api.error.something.went.wrong.please.try.after.sometime", null);
		}
	}

	public ApiResponse checkValidationForSubscriber(MobileOTPDto mobileOTPDto) throws UnknownHostException {
		logger.info("{}{} - Request received in checkValidationForSubscriber: {}", CLASS, Utility.getMethodName(),
				mobileOTPDto);
		int countDevice;
		int countMobile;
		int countEmail;
		SubscriberFcmToken fcmToken = new SubscriberFcmToken();
		SubscriberRegisterResponseDTO responseDTO = new SubscriberRegisterResponseDTO();
		SubscriberDevice deviceDetails = null;
		SubscriberDevice subscriberDeviceDetails = null;
		Subscriber previousSuid = null;
		try {
			if (mobileOTPDto.getOtpStatus()) {
				countDevice = subscriberRepoIface.countSubscriberDevice(mobileOTPDto.getDeviceId());
				logger.info("{}{} - Device count in checkValidationForSubscriber: {}, DeviceId: {}", CLASS,
						Utility.getMethodName(), countDevice, mobileOTPDto.getDeviceId());
				countMobile = subscriberRepoIface.countSubscriberMobile(mobileOTPDto.getSubscriberMobileNumber());
				logger.info("{}{} - Mobile count in checkValidationForSubscriber: {}, SubscriberMobileNumber: {}",
						CLASS, Utility.getMethodName(), countMobile, mobileOTPDto.getSubscriberMobileNumber());
				countEmail = subscriberRepoIface
						.countSubscriberEmailId(mobileOTPDto.getSubscriberEmail().toLowerCase());
				logger.info("{}{} - Email count in checkValidationForSubscriber: {}, SubscriberEmail: {}", CLASS,
						Utility.getMethodName(), countEmail, mobileOTPDto.getSubscriberEmail().toLowerCase());
				if (countEmail == 1 && countMobile == 1 && countDevice >= 1) {
					previousSuid = subscriberRepoIface.getSubscriberDetailsByEmailAndMobile(
							mobileOTPDto.getSubscriberEmail().toLowerCase(), mobileOTPDto.getSubscriberMobileNumber());
					if (previousSuid == null) {
						return exceptionHandlerUtil.createErrorResponse(
								"api.error.this.mobile.no.is.already.register.with.different.email.id");
					} else {
						SubscriberDevice subscriberDevice = deviceRepoIface
								.getSubscriber(previousSuid.getSubscriberUid());
						deviceDetails = deviceRepoIface.findBydeviceUidAndStatus(mobileOTPDto.getDeviceId(), "ACTIVE");
						if (subscriberDevice.getDeviceUid().equals(mobileOTPDto.getDeviceId())) {
							if (deviceDetails != null) {
								if (!subscriberDevice.getSubscriberUid().equals(deviceDetails.getSubscriberUid())) {
									return exceptionHandlerUtil.createErrorResponse(
											"api.error.this.device.is.already.register.with.differet.email.or.mobile.no");
								} else {
									countDevice = 1;
								}
							} else {
								countDevice = 1;
							}

						} else if (deviceDetails != null) {
							return exceptionHandlerUtil.createErrorResponse(
									"api.error.this.device.is.already.register.with.differet.email.or.mobile.no");
						} else {
							subscriberDeviceDetails = (SubscriberDevice) deviceRepoIface
									.findDeviceDetailsById(mobileOTPDto.getDeviceId());
							if (subscriberDeviceDetails.getDeviceStatus().equals(Constant.DEVICE_STATUS_ACTIVE)) {
								return exceptionHandlerUtil.createErrorResponse(
										"api.error.this.device.is.already.register.with.differet.email.or.mobile.no");
							} else {
								countDevice = 1;
							}
						}
					}

				}
				if (countDevice == 1 && countMobile == 1 && countDevice == 0) {
					countEmail = 1;
				}

				if (countDevice == 1 && countMobile == 1 && countEmail == 1) {
					logger.info("{}{} - All counts matched: countDevice == 1, countMobile == 1, countEmail == 1", CLASS,
							Utility.getMethodName());
					deviceDetails = deviceRepoIface.getSubscriber(previousSuid.getSubscriberUid());

					if (deviceDetails.getDeviceStatus().equals(Constant.DEVICE_STATUS_DISABLED)
							|| deviceDetails.getDeviceStatus() == Constant.DEVICE_STATUS_DISABLED) {
						logger.info("{}{} - This device is disabled", CLASS, Utility.getMethodName());
					}

					fcmToken = fcmTokenRepoIface.findBysubscriberUid(deviceDetails.getSubscriberUid());
					responseDTO.setSuID(deviceDetails.getSubscriberUid());
//					fcmToken.setSubscriberUid(deviceDetails.getSubscriberUid());
//					fcmToken.setCreatedDate(AppUtil.getDate());
					if (fcmToken == null) {
						fcmToken = new SubscriberFcmToken();
						fcmToken.setSubscriberUid(deviceDetails.getSubscriberUid());
						fcmToken.setCreatedDate(AppUtil.getDate());
					}
					fcmToken.setFcmToken(mobileOTPDto.getFcmToken());
					deviceDetails.setDeviceUid(mobileOTPDto.getDeviceId());
					deviceDetails.setDeviceStatus(Constant.DEVICE_STATUS_ACTIVE);
					deviceDetails.setUpdatedDate(AppUtil.getDate());

					Subscriber subscriber = subscriberRepoIface.findBysubscriberUid(deviceDetails.getSubscriberUid());

					if (!subscriber.getEmailId().equals(mobileOTPDto.getSubscriberEmail().toLowerCase())
							|| !subscriber.getMobileNumber().equals(mobileOTPDto.getSubscriberMobileNumber())) {
						logger.info("{}{} - This device is already registered with a different Email or Mobile No.",
								CLASS, Utility.getMethodName());
						return exceptionHandlerUtil.createErrorResponse(
								"api.error.this.device.is.already.register.with.differet.email.or.mobile.no");
					}
					SubscriberStatus subscriberStatus = statusRepoIface
							.findBysubscriberUid(deviceDetails.getSubscriberUid());
					if (subscriberStatus != null) {
						if (subscriberStatus.getSubscriberStatus() == Constant.SUBSCRIBER_STATUS
								|| subscriberStatus.getSubscriberStatus().equals(Constant.SUBSCRIBER_STATUS)) {
							responseDTO.setSubscriberStatus(Constant.SUBSCRIBER_STATUS);
						} else {
							responseDTO.setSubscriberStatus(subscriberStatus.getSubscriberStatus());
						}
					} else {
						if (!signRequired) {
							String consentData = "I agreed to above Terms and conditions and Data privacy terms";
							List<ConsentHistory> latestConsentList = consentHistoryRepo.findLatestConsent();
							ConsentHistory consentHistory = latestConsentList.isEmpty() ? null
									: latestConsentList.get(0);

							SubscriberConsents subscriberConsents = subscriberConsentsRepo
									.findSubscriberConsentBySuidAndConsentId(deviceDetails.getSubscriberUid(),
											consentHistory.getId());

//							List<ConsentHistory> latestConsents = consentHistoryRepo.findLatestConsent();
//							ConsentHistory consentHistory = latestConsents.isEmpty() ? null : latestConsents.get(0);
//
//							if (consentHistory != null) {
//								SubscriberConsents subscriberConsents = subscriberConsentsRepo
//										.findSubscriberConsentBySuidAndConsentId(deviceDetails.getSubscriberUid(),
//												consentHistory.getId());
							if (subscriberConsents == null) {
								SubscriberConsents subscriberConsents1 = new SubscriberConsents();
								subscriberConsents1.setConsentData(consentData);
								subscriberConsents1.setSuid(deviceDetails.getSubscriberUid());
								subscriberConsents1.setCreatedOn(AppUtil.getDate());
								subscriberConsents1.setConsentId(consentHistory.getId());
								subscriberConsentsRepo.save(subscriberConsents1);
							}
						}
						logger.info("{}{} - This device is already registered. Please continue.", CLASS,
								Utility.getMethodName());
						return exceptionHandlerUtil.createErrorResponseWithResult(
								"api.error.this.device.is.already.registered.please.continue", responseDTO);

					}
					if (!subscriberStatus.getSubscriberStatus().equals(Constant.SUBSCRIBER_STATUS)) {
						SubscriberOnboardingData subscriberOnboardingData = null;
						SubscriberDetails subscriberDetails = new SubscriberDetails();
						List<SubscriberOnboardingData> subscriberOnboardingDataList = onboardingDataRepoIface
								.getBySubUid(deviceDetails.getSubscriberUid());
						if (!subscriberOnboardingDataList.isEmpty()) {
							if (subscriberOnboardingDataList.size() > 1) {
								subscriberOnboardingData = findLatestOnboardedSub(subscriberOnboardingDataList);
							} else {
								subscriberOnboardingData = subscriberOnboardingDataList.get(0);
							}
						}

						if (subscriberOnboardingData != null) {
							String method = subscriberOnboardingData.getOnboardingMethod();
							SubscriberDTO subscriberDTO = new SubscriberDTO();
							subscriberDTO.setMethodName(method);

//							subscriberOnboardingData = (SubscriberOnboardingData) onboardingDataRepoIface
//									.findLatestSubscriber(subscriber.getSubscriberUid());

							subscriberOnboardingData = onboardingDataRepoIface
									.findLatestSubscriber(subscriber.getSubscriberUid()).stream().findFirst()
									.orElse(null);

							ApiResponse editTemplateDTORes = templateServiceIface
									.getTemplateLatestById(subscriberOnboardingData.getTemplateId());

							if (editTemplateDTORes.isSuccess()) {
								EditTemplateDTO editTemplateDTO = (EditTemplateDTO) editTemplateDTORes.getResult();
//                                String certStatus = String.valueOf(subscriberCertificatesRepoIface.getSubscriberCertificateStatus(
//                                        deviceDetails.getSubscriberUid(), Constant.SIGN, Constant.ACTIVE));

								List<String> statuses = subscriberCertificatesRepoIface.getSubscriberCertificateStatus(
										subscriber.getSubscriberUid(), Constant.SIGN, Constant.ACTIVE);

								String certStatus = statuses.isEmpty() ? null : statuses.get(0);

								subscriberDetails.setSubscriberName(subscriber.getFullName());
								subscriberDetails.setOnboardingMethod(method);
								subscriberDetails.setTemplateDetails(editTemplateDTO);
								subscriberDetails.setCertificateStatus(certStatus);
								PinStatus pinStatus = new PinStatus();
								if (certStatus != null) {
									if (certStatus.equals(Constant.ACTIVE)) {
										SubscriberCertificatePinHistory certificatePinHistory = subscriberCertPinHistoryRepoIface
												.findBysubscriberUid(deviceDetails.getSubscriberUid());
										if (certificatePinHistory != null) {
											if (certificatePinHistory.getAuthPinList() != null) {
												pinStatus.setAuthPinSet(true);
											}
											if (certificatePinHistory.getSignPinList() != null) {
												pinStatus.setSignPinSet(true);
											}
											subscriberDetails.setPinStatus(pinStatus);
										} else {
											subscriberDetails.setCertificateStatus(certStatus);
											subscriberDetails.setPinStatus(pinStatus);
										}
									} else {
										subscriberDetails.setCertificateStatus(certStatus);
										subscriberDetails.setPinStatus(pinStatus);
									}
								} else {
									subscriberDetails.setCertificateStatus(Constant.PENDING);
									subscriberDetails.setPinStatus(pinStatus);
								}
							} else {
								subscriberDetails = null;
							}
							responseDTO.setSubscriberDetails(subscriberDetails);
						} else {
							responseDTO.setSubscriberDetails(null);
						}
					} else {
						responseDTO.setSubscriberDetails(null);
					}

					List<String> paymentStatus = subscriberRepoIface
							.subscriberPaymnetStatus(subscriber.getSubscriberUid());
					if (paymentStatus.size() != 0) {
						responseDTO.setOnboardingPaymentStatus(paymentStatus.get(0));
					} else {
						responseDTO.setOnboardingPaymentStatus(Constant.PAYMENT_STATUS_PENDING);
					}

					List<String> firstTimeOnboardingList = subscriberRepoIface
							.firstTimeOnboardingPaymentStatus(subscriber.getSubscriberUid());

					if (!firstTimeOnboardingList.isEmpty()) {
						responseDTO.setFirstTimeOnboarding(false);
					} else {
						responseDTO.setFirstTimeOnboarding(true);
					}

					fcmToken = fcmTokenRepoIface.save(fcmToken);
					deviceDetails = deviceRepoIface.save(deviceDetails);
					if (!signRequired) {
						String consentData = "I agreed to above Terms and conditions and Data privacy terms";
						List<ConsentHistory> latestConsentList = consentHistoryRepo.findLatestConsent();

						ConsentHistory consentHistory = latestConsentList.get(0);

						SubscriberConsents subscriberConsents = subscriberConsentsRepo
								.findSubscriberConsentBySuidAndConsentId(deviceDetails.getSubscriberUid(),
										consentHistory.getId());
						if (subscriberConsents == null) {
							SubscriberConsents subscriberConsents1 = new SubscriberConsents();
							subscriberConsents1.setConsentData(consentData);
							subscriberConsents1.setSuid(deviceDetails.getSubscriberUid());
							subscriberConsents1.setCreatedOn(AppUtil.getDate());
							subscriberConsents1.setConsentId(consentHistory.getId());
							subscriberConsentsRepo.save(subscriberConsents1);
						}
					}
					logger.info("{}{} - This device is already registered. Please continue.", CLASS,
							Utility.getMethodName());
					return exceptionHandlerUtil.createErrorResponseWithResult(
							"api.error.this.device.is.already.registered.please.continue", responseDTO);
				}
				if (countDevice >= 1) {
					deviceDetails = deviceRepoIface.findBydeviceUidAndStatus(mobileOTPDto.getDeviceId(), "ACTIVE");
					logger.info("{}{} - deviceDetails: {}", CLASS, Utility.getMethodName(), deviceDetails);
					if (deviceDetails != null) {
						if (countDevice >= 1 && countEmail == 0) {
							logger.info("{}{} - This device is already registered with a different email", CLASS,
									Utility.getMethodName());
							return exceptionHandlerUtil.createErrorResponse(
									"api.error.this.device.is.already.registered.with.different.email");
						}
						if (countDevice >= 1 && countMobile == 0) {
							logger.info("{}{} - This device is already registered with a different mobile number",
									CLASS, Utility.getMethodName());
							return exceptionHandlerUtil.createErrorResponse(
									"api.error.this.device.is.already.register.with.different.mobile.number");
						}
					}
				}

				if (countDevice == 0 && countEmail == 1) {
					Subscriber subscriber = subscriberRepoIface.getSubscriberUidByEmailAndMobile(
							mobileOTPDto.getSubscriberEmail(), mobileOTPDto.getSubscriberMobileNumber());

					SubscriberDevice device = deviceRepoIface.getSubscriber(subscriber.getSubscriberUid());

					if (device.getDeviceStatus().equals(Constant.DEVICE_STATUS_ACTIVE)) {
						if (countEmail == 1) {
							logger.error(
									"{}{} - Error: This email ID is already registered with a different device. Please deactivate the other device.",
									CLASS, Utility.getMethodName());
							return exceptionHandlerUtil.createErrorResponse(
									"api.error.this.email.id.is.already.register.with.different.device.please.deactivate.the.other.device");
						}
					}
				} else if (countDevice == 0 && countMobile == 1) {
					Subscriber subscriber = subscriberRepoIface.getSubscriberUidByEmailAndMobile(
							mobileOTPDto.getSubscriberEmail(), mobileOTPDto.getSubscriberMobileNumber());

					SubscriberDevice device = deviceRepoIface.getSubscriber(subscriber.getSubscriberUid());

					if (device.getDeviceStatus().equals(Constant.DEVICE_STATUS_ACTIVE)) {
						if (countMobile == 1) {
							return exceptionHandlerUtil.createErrorResponse(
									"api.error.this.mobile.no.is.already.register.with.different.device.please.deactivate.the.other.device");
						}
					}

				}
				if (countDevice == 0) {
					int activeDeviceCount = subscriberCompleteDetailRepoIface
							.getActiveDeviceCountStatusByEmailAndMobileNo(Constant.ACTIVE,
									mobileOTPDto.getSubscriberEmail(), mobileOTPDto.getSubscriberMobileNumber());
					if (activeDeviceCount != 0) {
						if (countDevice == 0 && countEmail == 1) {
							logger.info(
									"{}{} - This email ID is already registered with a different device. Please deactivate the other device.",
									CLASS, Utility.getMethodName());
							return exceptionHandlerUtil.createErrorResponse(
									"api.error.this.email.id.is.already.register.with.different.device.please.deactivate.the.other.device");
						}
						if (countDevice == 0 && countMobile == 1) {
							logger.info(
									"{}{} - This mobile number is already registered with a different device. Please deactivate the other device.",
									CLASS, Utility.getMethodName());
							return exceptionHandlerUtil.createErrorResponse(
									"api.error.this.mobile.no.is.already.register.with.different.device.please.deactivate.the.other.device");
						} else {
							return exceptionHandlerUtil.successResponse("api.response.success");
						}
					} else {
						return exceptionHandlerUtil.successResponse("api.response.success");
					}
				} else {
					return exceptionHandlerUtil.successResponse("api.response.success");
				}
			} else {
				logger.info("{}{} - OTP verification failed", CLASS, Utility.getMethodName());
				return exceptionHandlerUtil.createErrorResponse("api.error.otp.verification.is.failed");
			}
		} catch (Exception e) {
			logger.error("{}{} - Exception: {}", CLASS, Utility.getMethodName(), e.getMessage());
			e.printStackTrace();
			sentryClientExceptions.captureTags(mobileOTPDto.getSuID(), mobileOTPDto.getSubscriberMobileNumber(),
					"checkValidationSubscriber", "SubscriberController");
			sentryClientExceptions.captureExceptions(e);
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Override
	public ApiResponse saveSubscriberDocument(SubscriberDocumentDto subscriberDocumentDto) {
		try {
			SubscriberPersonalDocument personalDocument = subscriberPersonalDocumentRepo
					.findBySubscriberUniqueId(subscriberDocumentDto.getSubscriberUID());

			if (personalDocument != null) {
				personalDocument.setSubscriberUniqueId(subscriberDocumentDto.getSubscriberUID());
				personalDocument.setDocument(subscriberDocumentDto.getDocument());
				personalDocument.setUpdatedDate(AppUtil.getDate());
				subscriberPersonalDocumentRepo.save(personalDocument);

				return exceptionHandlerUtil.successResponse("api.response.subscriber.document.updated.successfully");
			} else {
				SubscriberPersonalDocument subscriberPersonalDocument = new SubscriberPersonalDocument();
				subscriberPersonalDocument.setSubscriberUniqueId(subscriberDocumentDto.getSubscriberUID());
				subscriberPersonalDocument.setDocument(subscriberDocumentDto.getDocument());
				subscriberPersonalDocument.setCreatedDate(AppUtil.getDate());
				subscriberPersonalDocument.setUpdatedDate(AppUtil.getDate());
				subscriberPersonalDocumentRepo.save(subscriberPersonalDocument);

				return exceptionHandlerUtil.successResponse("api.response.subscriber.document.save.successfully");
			}

		} catch (JDBCConnectionException | ConstraintViolationException | DataException | LockAcquisitionException
				| PessimisticLockException | QueryTimeoutException | SQLGrammarException | GenericJDBCException e) {
			e.printStackTrace();
			return exceptionHandlerUtil.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");
		} catch (Exception e) {
			e.printStackTrace();
			return exceptionHandlerUtil.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");

		}
	}

	@SuppressWarnings("null")
	@Override
	public ApiResponse addSubscriberObData(SubscriberObRequestDTO obRequestDTO) throws Exception {
		try {
			if (Objects.isNull(obRequestDTO)) {
				return exceptionHandlerUtil
						.createErrorResponse("api.error.subscriber.ob.request.cant.be.null.or.empty");
			}

			String validationMessage = ValidationUtil.validate(obRequestDTO);
			if (validationMessage != null) {
				System.out.println(" addSubscriberObData Validation errors: " + validationMessage);
				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(validationMessage, null);
			}

			String subscriberData = ValidationUtil.validate(obRequestDTO.getSubscriberData());
			if (subscriberData != null) {
				System.out.println(" addSubscriberObData Validation errors: " + subscriberData);
				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(subscriberData, null);
			}

			Date startTime = new Date();
			SubscriberObData subscriberObData = new SubscriberObData();
			SubscriberObData additionalFile = new SubscriberObData();
			Subscriber subscriber = new Subscriber();
			Subscriber savedSubscriber = new Subscriber();
			SubscriberOnboardingData onboardingData = new SubscriberOnboardingData();
			SubscriberDevice subscriberDevice = new SubscriberDevice();
			SubscriberRaData raData = new SubscriberRaData();
			SubscriberStatus status = new SubscriberStatus();
			IssueCertDTO issueCertDTO = new IssueCertDTO();
			int idDocNumberCount;
			String subscriberStatus = null;
			subscriber = subscriberRepoIface.findBysubscriberUid(obRequestDTO.getSuID());
			if (Objects.isNull(subscriber)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.not.found");
			}

			subscriberObData = obRequestDTO.getSubscriberData();

			if (!isOnboardingFee) {
				if (obRequestDTO.getSubscriberType().equals("Citizen")
						&& obRequestDTO.getSubscriberType() == "Citizen") {
					if (obRequestDTO.getSubscriberData().getOptionalData1() != null
							&& !obRequestDTO.getSubscriberData().getOptionalData1().isEmpty()) {
						int count = isOptionData1Present(obRequestDTO.getSubscriberData().getOptionalData1());
						if (count == 1) {
							String suid = onboardingDataRepoIface
									.getOptionalData1Subscriber(obRequestDTO.getSubscriberData().getOptionalData1());
							if (!suid.equals(obRequestDTO.getSuID())) {
								logger.info(
										"{}{} - addSubscriberObData isOptionData1Present: Onboarding cannot be processed because the same national ID already exists: {}",
										CLASS, Utility.getMethodName(), count);
								return exceptionHandlerUtil.createErrorResponse(
										"api.error.onboarding.can.not.be.processed.because.the.same.national.id.already.exists");
							}
						}
					} else {
						return exceptionHandlerUtil.createErrorResponse("api.error.optional.data.is.empty");
					}
				}
			} else {
				if (!obRequestDTO.getSubscriberType().equals(Constant.RESIDENT)
						&& obRequestDTO.getSubscriberType() != Constant.RESIDENT) {
					if (obRequestDTO.getSubscriberData().getOptionalData1() != null
							&& !obRequestDTO.getSubscriberData().getOptionalData1().isEmpty()) {
						int count = isOptionData1Present(obRequestDTO.getSubscriberData().getOptionalData1());
						if (count == 1) {
							String suid = onboardingDataRepoIface
									.getOptionalData1Subscriber(obRequestDTO.getSubscriberData().getOptionalData1());
							if (!suid.equals(obRequestDTO.getSuID())) {
								logger.info(
										"{}{} - addSubscriberObData isOptionData1Present: Onboarding cannot be processed because the same national ID already exists: {}",
										CLASS, Utility.getMethodName(), count);
								return exceptionHandlerUtil.createErrorResponse(
										"api.error.onboarding.can.not.be.processed.because.the.same.national.id.already.exists");
							}
						}
					} else {
						return exceptionHandlerUtil.createErrorResponse("api.error.optional.data.is.empty");
					}
				}
			}

			subscriberStatus = subscriberRepoIface.getSubscriberStatus(obRequestDTO.getSuID());
			logger.info("{}{} - addSubscriberObData request for subscriberStatus: {}", CLASS, Utility.getMethodName(),
					subscriberStatus);
			if (Objects.isNull(subscriberStatus)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.not.found");
			}
			subscriberDevice = deviceRepoIface.getSubscriber(obRequestDTO.getSuID());
			if (subscriberDevice.getDeviceStatus().equals(Constant.DEVICE_STATUS_DISABLED)
					|| subscriberDevice.getDeviceStatus() == Constant.DEVICE_STATUS_DISABLED) {
				logger.info("{}{} - subscriberDevice DEVICE_STATUS_DISABLED: {}", CLASS, Utility.getMethodName(),
						subscriberDevice);
				return exceptionHandlerUtil.createErrorResponse("api.error.this.device.is.disabled");
			}
			int idDocCount = subscriberRepoIface.getIdDocCount(subscriberObData.getDocumentNumber());
			idDocNumberCount = subscriberRepoIface.getSubscriberIdDocNumber(subscriberObData.getDocumentNumber(),
					obRequestDTO.getSuID());
			if (idDocCount > 0) {
				if (idDocCount > 0 && idDocNumberCount == 0) {
					return exceptionHandlerUtil.createErrorResponse("api.error.this.document.is.already.onboarded");
				}
			}

			String fullName = Stream
					.of(subscriberObData.getSecondaryIdentifier(), subscriberObData.getPrimaryIdentifier())
					.filter(s -> s != null && !s.trim().isEmpty()) // skip null or empty values
					.map(s -> s.replaceAll("\\s+", " ").trim()) // clean spaces properly
					.collect(Collectors.joining(" "));

			raData.setCommonName(fullName);

//			subscriber.setFullName(subscriberObData.getSecondaryIdentifier().trim() + " "
//					+ subscriberObData.getPrimaryIdentifier().trim());

			subscriber.setFullName(fullName);
			subscriber.setDateOfBirth(subscriberObData.getDateOfBirth());
			subscriber.setIdDocType(subscriberObData.getDocumentType());
			subscriber.setIdDocNumber(subscriberObData.getDocumentNumber());
			subscriber.setUpdatedDate(AppUtil.getDate());
			subscriber.setSubscriberUid(obRequestDTO.getSuID());
			if (!obRequestDTO.getSubscriberType().equals(Constant.RESIDENT)
					&& obRequestDTO.getSubscriberType() != Constant.RESIDENT) {
				// subscriber.setNationalId(obRequestDTO.getSubscriberData().getOptionalData1());
				subscriber.setNationalId(subscriberObData.getOptionalData1());
			}

			onboardingData.setCreatedDate(AppUtil.getDate());
			onboardingData.setIdDocType(subscriberObData.getDocumentType());
			onboardingData.setIdDocNumber(subscriberObData.getDocumentNumber());
			onboardingData.setOnboardingMethod(obRequestDTO.getOnboardingMethod());
			onboardingData.setSubscriberUid(obRequestDTO.getSuID());
			onboardingData.setTemplateId(obRequestDTO.getTemplateId());
			onboardingData.setOnboardingMethod(obRequestDTO.getOnboardingMethod());
			onboardingData.setSubscriberType(obRequestDTO.getSubscriberType());
			onboardingData.setIdDocCode(subscriberObData.getDocumentCode());
			onboardingData.setGender(subscriberObData.getGender());
			onboardingData.setGeolocation(subscriberObData.getGeoLocation());
			// onboardingData.setOptionalData1(subscriberObData.getOptionalData1());

			if (subscriberObData.getOptionalData1() != null && !subscriberObData.getOptionalData1().isEmpty()
					&& !subscriberObData.getOptionalData1().equals("0")) {
				onboardingData.setOptionalData1(subscriberObData.getOptionalData1());
			} else {
				onboardingData.setOptionalData1(subscriberObData.getDocumentNumber());
			}

			onboardingData.setDateOfExpiry(subscriberObData.getDateOfExpiry());

//			if (subscriberObData.getNiraResponse() != null && !subscriberObData.getNiraResponse().isEmpty()) {
//				Result result = DAESService.decryptSecureWireData(subscriberObData.getNiraResponse());
//				String s = new String(result.getResponse());
//
//				JsonNode jsonNode = objectMapper.readTree(s);
//				// System.out.println(" jsonNode :::: " + jsonNode);
//				// Extract the "photo" field
//				String photo = jsonNode.get("authenticPhoto").asText();
//				// String uaeKycId = jsonNode.get("UaeKycId").asText(null);
//
//				if (!isOnboardingFee) {
//					String uaeKycId = jsonNode.path("uaeKycId").asText(null);
//					System.out.println(" uaeKycId :::: " + uaeKycId);
//					if (uaeKycId != null) {
//						onboardingData.setUaeKycId(uaeKycId);
//					}
//				}
//
//				onboardingData.setVerifierProvidedPhoto(photo);
//			} else {
//				onboardingData.setVerifierProvidedPhoto(obRequestDTO.getSubscriberData().getSubscriberSelfie());
//			}

			if (subscriberObData.getNiraResponse() != null && !subscriberObData.getNiraResponse().isEmpty()) {
				String photo = null;
				// String personFace = null;

				if (isOnboardingFee) {

					Result result = DAESService.decryptSecureWireData(subscriberObData.getNiraResponse());
					String s = new String(result.getResponse());
					JsonNode jsonNode = objectMapper.readTree(s);
					photo = jsonNode.get("authenticPhoto").asText();
				} else {
					JsonNode root = objectMapper.readTree(subscriberObData.getNiraResponse());

					// JsonNode dataNode = root.path("customerDetails").path("result").path("data");
					JsonNode dataNode = root.path("customerDetails").path("Result").path("Data");
					// uaeKycId
					String uaeKycId = dataNode.path("UaeKycId").asText(null);
					System.out.println("uaeKycId = " + uaeKycId);
					onboardingData.setUaeKycId(uaeKycId);
					// documents -> personFace
					photo = dataNode.path("Documents").path("PersonFace").asText(null);
					// ✅ generate hash using Data json
					String hash = AppUtil.hmacSha256Base64(root.toString());

					onboardingData.setDocumentResponseHash(hash);
					onboardingData.setNiraResponse(root.toString());

					String passportNumber = dataNode.path("ActivePassport").path("DocumentNo").asText(null);
					System.out.println(" passport number ::" + passportNumber);
					if (passportNumber != null && !passportNumber.trim().isEmpty()) {
						subscriber.setPassportNumber(passportNumber);
					}

					String emiratesIdNumber = dataNode.path("ResidenceInfo").path("EmiratesIdNumber").asText(null);
					System.out.println(" EmiratesIdNumber  ::" + emiratesIdNumber);

					if (emiratesIdNumber != null && !emiratesIdNumber.trim().isEmpty()) {
						subscriber.setNationalIdNumber(emiratesIdNumber);
					}

					String emiratesIdDocumentNumber = dataNode.path("ResidenceInfo").path("DocumentNo").asText(null);
					System.out.println(" EmiratesIdDocumentNumber number ::" + emiratesIdDocumentNumber);

					if (emiratesIdDocumentNumber != null && !emiratesIdDocumentNumber.trim().isEmpty()) {
						subscriber.setNationalIdCardNumber(emiratesIdDocumentNumber);
					}

					if (obRequestDTO.getOnboardingMethod().equalsIgnoreCase("NIN")) {

						if (emiratesIdNumber != null && !emiratesIdNumber.trim().isEmpty()) {

							subscriber.setIdDocNumber(emiratesIdNumber);
							subscriber.setNationalId(emiratesIdNumber);
						}

					} else if (obRequestDTO.getOnboardingMethod().equalsIgnoreCase("PASSPORT")) {
						if (passportNumber != null && !passportNumber.trim().isEmpty()) {
							subscriber.setIdDocNumber(passportNumber);
						}
					}
				}
				// onboardingData.setVerifierProvidedPhoto(photo);
				onboardingData.setVerifierProvidedPhoto(photo);
			} else {
				onboardingData.setVerifierProvidedPhoto(obRequestDTO.getSubscriberData().getSubscriberSelfie());
			}

			// Set nira Response
			Optional.ofNullable(subscriberObData.getNiraResponse()).filter(response -> !response.isEmpty())
					.ifPresent(onboardingData::setNiraResponse);

			// set LOA based on onboarding method
			OnboardingMethod onboardingMethod = onBoardingMethodRepoIface
					.findByonboardingMethod(obRequestDTO.getOnboardingMethod());
			onboardingData.setLevelOfAssurance(onboardingMethod.getLevelOfAssurance());

//			raData.setCommonName(subscriberObData.getSecondaryIdentifier().trim() + " "
//					+ subscriberObData.getPrimaryIdentifier().trim());

			raData.setCommonName(fullName);
			raData.setCertificateType(Constant.BOTH);
			raData.setCountryName(subscriberObData.getNationality());
			raData.setCreatedDate(AppUtil.getDate());
//			raData.setPkiPassword(
//					subscriberObData.getSecondaryIdentifier() + " " + subscriberObData.getPrimaryIdentifier());
			raData.setPkiPassword(fullName);
			raData.setPkiPasswordHash(subscriberObData.getSecondaryIdentifier() + " "
					+ subscriberObData.getPrimaryIdentifier().hashCode());
//			raData.setPkiUserName(
//					subscriberObData.getSecondaryIdentifier() + " " + subscriberObData.getPrimaryIdentifier());
			raData.setPkiUserName(fullName);
			raData.setPkiUserNameHash(subscriberObData.getSecondaryIdentifier() + " "
					+ subscriberObData.getPrimaryIdentifier().hashCode());

			raData.setSubscriberUid(obRequestDTO.getSuID());

			issueCertDTO.setSubscriberUniqueId(obRequestDTO.getSuID());

			FaceFeaturesDto faceFeaturesDto = new FaceFeaturesDto();
			faceFeaturesDto.setSubscriberPhoto(obRequestDTO.getSubscriberData().getSubscriberSelfie());
			Selfie selfie = new Selfie();

			// Save Nira response photo in place of selfie
//			if(subscriberObData.getNiraResponse() != null && !subscriberObData.getNiraResponse().isEmpty()) {
//
//				Result result = DAESService.decryptSecureWireData(subscriberObData.getNiraResponse());
//				String niraSelfie = new String(result.getResponse());
//				selfie.setSubscriberSelfie(niraSelfie);
//			}else {

			// Remove BackGround image
			if (removeBackGroundFromImageBoolean) {
				ApiResponse apiResponseGetImage = getImageWithOutBackGround(
						obRequestDTO.getSubscriberData().getDocumentNumber(),
						obRequestDTO.getSubscriberData().getSubscriberSelfie());

				if (apiResponseGetImage.getResult() == null) {
					selfie.setSubscriberSelfie(obRequestDTO.getSubscriberData().getSubscriberSelfie());
				} else {
					selfie.setSubscriberSelfie(apiResponseGetImage.getResult().toString());
				}
			} else {
				selfie.setSubscriberSelfie(obRequestDTO.getSubscriberData().getSubscriberSelfie());
			}

			//
			selfie.setSubscriberUniqueId(obRequestDTO.getSuID());
			// for saving selfie in EDMS
//            CompletableFuture<ApiResponse> selfieResponse = edmsService.saveFileToEdms(selfie, "selfie", null);
			// for saving selfie in minIO

			if (isOnboardingFee) {
				// UgPASS
				CompletableFuture<ApiResponse> selfieResponse = minioStorageService.saveFileToMinio(selfie, "selfie",
						null);

				ApiResponse apiResponse = selfieResponse.get();
				if (apiResponse.isSuccess()) {
					logger.info(CLASS + " addSubscriberObData res for saveFileToEdms: ", selfieResponse);
					String selfieURI = (String) apiResponse.getResult();
					onboardingData.setSelfieUri(selfieURI);
				} else {
					logger.info(CLASS + "addSubscriberObData res in false for saveFileToEdms: ",
							apiResponse.getMessage());
					return exceptionHandlerUtil.createFailedResponseWithCustomMessage(apiResponse.getMessage(), null);

				}
			} else {
				// UAEID EDMS remove in UAEID
				onboardingData.setSelfie(obRequestDTO.getSubscriberData().getSubscriberSelfie());
			}

			CompletableFuture<ApiResponse> selfieThumbnailResponse = minioStorageService
					.createThumbnailOfSelfie(selfie);

			ApiResponse selfieApiResponse = selfieThumbnailResponse.get();
			if (selfieApiResponse.isSuccess()) {
				logger.info(CLASS + "addSubscriberObData res for createThumlbnailOfSelfie: ",
						selfieApiResponse.isSuccess());
				onboardingData.setSelfieThumbnailUri(selfieApiResponse.getResult().toString());
			} else {
				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(selfieApiResponse.getMessage(), null);

			}

			savedSubscriber = subscriberRepoIface.save(subscriber);

			additionalFile = subscriberObData;
			additionalFile.setSubscriberSelfie(null);
			additionalFile.setSubscriberUniqueId(onboardingData.getSubscriberUid());
			String additionalFieldSaved = objectMapper.writeValueAsString(additionalFile);
			onboardingData.setOnboardingDataFieldsJson(additionalFieldSaved);
			onboardingData.setRemarks(obRequestDTO.getSubscriberData().getRemarks());

			faceFeaturesDto.setSuid(onboardingData.getSubscriberUid());
			faceFeaturesDto.setSubscriberName(savedSubscriber.getFullName());
			faceFeaturesDto.setSubscriberDataJson(additionalFieldSaved);
			// for faceFeatures rest call
			// saveFaceFeaturesAsync(faceFeaturesDto,registerFaceURL, restTemplate);

			if (registerFaceBoolean) {
				try {
					ExecutorService executor1 = Executors.newFixedThreadPool(1000);
					Runnable registerFaceWorkerThread = new RegisterFaceWorkerThread(registerFaceURL, faceFeaturesDto);
					executor1.execute(registerFaceWorkerThread);
					executor1.shutdown();
				} catch (Exception e) {
				}
			}

			onboardingData = onboardingDataRepoIface.save(onboardingData);
			status = statusRepoIface.findBysubscriberUid(onboardingData.getSubscriberUid());

			String subStatus = subscriberRepoIface.getSubscriberStatus(onboardingData.getSubscriberUid());
			logger.info("{}{} - getSubscriberStatus: {}", CLASS, Utility.getMethodName(), subStatus);
			if (subStatus != null) {
				if (subStatus.equals(Constant.ACTIVE)) {
					status.setSubscriberStatus(Constant.ACTIVE);
					status.setSubscriberStatusDescription(Constant.LOA_UPDATED);
					status.setUpdatedDate(AppUtil.getDate());
					status = statusRepoIface.save(status);
				} else if (subStatus.equals(Constant.PIN_SET_REQUIRED)) {
					status.setSubscriberStatus(Constant.PIN_SET_REQUIRED);
					status.setSubscriberStatusDescription(Constant.LOA_UPDATED);
					status.setUpdatedDate(AppUtil.getDate());
					status = statusRepoIface.save(status);
				} else {

					if (isOnboardingFee) {
						System.out.println("isOnboardingFee false :::::" + isOnboardingFee);
						status.setSubscriberStatus("ONBOARDED");
					} else {
						System.out.println("It should come here and make it ACTIVE");
						System.out.println("COnstant active value:::ACTIVE");
						status.setSubscriberStatus("ACTIVE");
						System.out.println("Status after changed::::" + status.getSubscriberStatus());
					}
					// status.setSubscriberStatus(Constant.ONBOARDED);
					status.setSubscriberStatusDescription(Constant.ONBOARDED_SUCESSFULLY);
					status.setUpdatedDate(AppUtil.getDate());
					status = statusRepoIface.save(status);
					raData = raRepoIface.save(raData);
				}
			} else {
				if (isOnboardingFee) {
					System.out.println("isOnboardingFee false :::::" + isOnboardingFee);
					status.setSubscriberStatus("ONBOARDED");
				} else {
					System.out.println("It should come here and make it ACTIVE");
					System.out.println("COnstant active value:::ACTIVE");
					status.setSubscriberStatus("ACTIVE");
					System.out.println("Status after changed::::" + status.getSubscriberStatus());
				}
				// status.setSubscriberStatus(Constant.ONBOARDED);
				status.setSubscriberStatusDescription(Constant.ONBOARDED_SUCESSFULLY);
				status.setUpdatedDate(AppUtil.getDate());
				status = statusRepoIface.save(status);
				raData = raRepoIface.save(raData);
			}

			Subscriber s = subscriberRepoIface.findBysubscriberUid(onboardingData.getSubscriberUid());
			if (s != null) {
//				try {
//					ExecutorService executor = Executors.newFixedThreadPool(1000);
//					Runnable visitorWorkerThread = new GenerateVisitorCardWorkerThread(visitorCardUrl, s, raData,
//							onboardingData);
//					executor.execute(visitorWorkerThread);
//					executor.shutdown();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}

				Date endTime = new Date();
				double toatlTime = AppUtil.getDifferenceInSeconds(startTime, endTime);
				logModelServiceImpl.setLogModel(true, s.getSubscriberUid(), onboardingData.getGeolocation(),
						Constant.SUBSCRIBER_ONBOARDED, s.getSubscriberUid(), String.valueOf(toatlTime), startTime,
						endTime, null);
				logger.info("{}{} - Subscriber OnBoarding Data Saved: {}", CLASS, Utility.getMethodName(), s);
				return exceptionHandlerUtil
						.createSuccessResponse("api.response.ugpass.application.submitted.successfully", s);
			} else {
				logModelServiceImpl.setLogModel(false, s.getSubscriberUid(), null, Constant.SUBSCRIBER_ONBOARDED,
						s.getSubscriberUid(), null, null, null, null);
				return exceptionHandlerUtil
						.createErrorResponseWithResult("api.error.ugpass.application.submission.failed", s);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("{}{} - Subscriber OnBoarding Data Exception: {}", CLASS, Utility.getMethodName(), e);
			sentryClientExceptions.captureExceptions(e);
			return onBoardingServiceException.handleExceptionWithStaticMessageWithSentry(e, obRequestDTO.getSuID());
		}
	}

	/*
	 * public ApiResponse addSubscriberObData(SubscriberObRequestDTO obRequestDTO)
	 * throws Exception { try { if (Objects.isNull(obRequestDTO)) { return
	 * exceptionHandlerUtil
	 * .createErrorResponse("api.error.subscriber.ob.request.cant.be.null.or.empty")
	 * ; }
	 * 
	 * String validationMessage = ValidationUtil.validate(obRequestDTO); if
	 * (validationMessage != null) {
	 * System.out.println(" addSubscriberObData Validation errors: " +
	 * validationMessage); return
	 * exceptionHandlerUtil.createFailedResponseWithCustomMessage(validationMessage,
	 * null); }
	 * 
	 * String subscriberData =
	 * ValidationUtil.validate(obRequestDTO.getSubscriberData()); if (subscriberData
	 * != null) { System.out.println(" addSubscriberObData Validation errors: " +
	 * subscriberData); return
	 * exceptionHandlerUtil.createFailedResponseWithCustomMessage(subscriberData,
	 * null); }
	 * 
	 * Date startTime = new Date(); SubscriberObData subscriberObData = new
	 * SubscriberObData(); SubscriberObData additionalFile = new SubscriberObData();
	 * Subscriber subscriber = new Subscriber(); Subscriber savedSubscriber = new
	 * Subscriber(); SubscriberOnboardingData onboardingData = new
	 * SubscriberOnboardingData(); SubscriberDevice subscriberDevice = new
	 * SubscriberDevice(); SubscriberRaData raData = new SubscriberRaData();
	 * SubscriberStatus status = new SubscriberStatus(); IssueCertDTO issueCertDTO =
	 * new IssueCertDTO(); int idDocNumberCount; String subscriberStatus = null;
	 * subscriber = subscriberRepoIface.findBysubscriberUid(obRequestDTO.getSuID());
	 * if (Objects.isNull(subscriber)) { return
	 * exceptionHandlerUtil.createErrorResponse("api.error.subscriber.not.found"); }
	 * 
	 * subscriberObData = obRequestDTO.getSubscriberData(); if
	 * (!obRequestDTO.getSubscriberType().equals(Constant.RESIDENT) &&
	 * obRequestDTO.getSubscriberType() != Constant.RESIDENT) { if
	 * (obRequestDTO.getSubscriberData().getOptionalData1() != null &&
	 * !obRequestDTO.getSubscriberData().getOptionalData1().isEmpty()) { int count =
	 * isOptionData1Present(obRequestDTO.getSubscriberData().getOptionalData1()); if
	 * (count == 1) { String suid = onboardingDataRepoIface
	 * .getOptionalData1Subscriber(obRequestDTO.getSubscriberData().getOptionalData1
	 * ()); if (!suid.equals(obRequestDTO.getSuID())) { logger.
	 * info("{}{} - addSubscriberObData isOptionData1Present: Onboarding cannot be processed because the same national ID already exists: {}"
	 * , CLASS, Utility.getMethodName(), count); return
	 * exceptionHandlerUtil.createErrorResponse(
	 * "api.error.onboarding.can.not.be.processed.because.the.same.national.id.already.exists"
	 * ); } } } else { return
	 * exceptionHandlerUtil.createErrorResponse("api.error.optional.data.is.empty");
	 * } }
	 * 
	 * subscriberStatus =
	 * subscriberRepoIface.getSubscriberStatus(obRequestDTO.getSuID());
	 * logger.info("{}{} - addSubscriberObData request for subscriberStatus: {}",
	 * CLASS, Utility.getMethodName(), subscriberStatus); if
	 * (Objects.isNull(subscriberStatus)) { return
	 * exceptionHandlerUtil.createErrorResponse("api.error.subscriber.not.found"); }
	 * subscriberDevice = deviceRepoIface.getSubscriber(obRequestDTO.getSuID()); if
	 * (subscriberDevice.getDeviceStatus().equals(Constant.DEVICE_STATUS_DISABLED)
	 * || subscriberDevice.getDeviceStatus() == Constant.DEVICE_STATUS_DISABLED) {
	 * logger.info("{}{} - subscriberDevice DEVICE_STATUS_DISABLED: {}", CLASS,
	 * Utility.getMethodName(), subscriberDevice); return
	 * exceptionHandlerUtil.createErrorResponse("api.error.this.device.is.disabled")
	 * ; } int idDocCount =
	 * subscriberRepoIface.getIdDocCount(subscriberObData.getDocumentNumber());
	 * idDocNumberCount =
	 * subscriberRepoIface.getSubscriberIdDocNumber(subscriberObData.
	 * getDocumentNumber(), obRequestDTO.getSuID()); if (idDocCount > 0) { if
	 * (idDocCount > 0 && idDocNumberCount == 0) { return
	 * exceptionHandlerUtil.createErrorResponse(
	 * "api.error.this.document.is.already.onboarded"); } } subscriber.setFullName(
	 * subscriberObData.getSecondaryIdentifier() + " " +
	 * subscriberObData.getPrimaryIdentifier());
	 * subscriber.setDateOfBirth(subscriberObData.getDateOfBirth());
	 * subscriber.setIdDocType(subscriberObData.getDocumentType());
	 * subscriber.setIdDocNumber(subscriberObData.getDocumentNumber());
	 * subscriber.setUpdatedDate(AppUtil.getDate());
	 * subscriber.setSubscriberUid(obRequestDTO.getSuID()); if
	 * (!obRequestDTO.getSubscriberType().equals(Constant.RESIDENT) &&
	 * obRequestDTO.getSubscriberType() != Constant.RESIDENT) {
	 * subscriber.setNationalId(obRequestDTO.getSubscriberData().getOptionalData1())
	 * ; }
	 * 
	 * onboardingData.setCreatedDate(AppUtil.getDate());
	 * onboardingData.setIdDocType(subscriberObData.getDocumentType());
	 * onboardingData.setIdDocNumber(subscriberObData.getDocumentNumber());
	 * onboardingData.setOnboardingMethod(obRequestDTO.getOnboardingMethod());
	 * onboardingData.setSubscriberUid(obRequestDTO.getSuID());
	 * onboardingData.setTemplateId(obRequestDTO.getTemplateId());
	 * onboardingData.setOnboardingMethod(obRequestDTO.getOnboardingMethod());
	 * onboardingData.setSubscriberType(obRequestDTO.getSubscriberType());
	 * onboardingData.setIdDocCode(subscriberObData.getDocumentCode());
	 * onboardingData.setGender(subscriberObData.getGender());
	 * onboardingData.setGeolocation(subscriberObData.getGeoLocation());
	 * onboardingData.setOptionalData1(subscriberObData.getOptionalData1());
	 * onboardingData.setDateOfExpiry(subscriberObData.getDateOfExpiry());
	 * 
	 * if (subscriberObData.getNiraResponse() != null &&
	 * !subscriberObData.getNiraResponse().isEmpty()) { Result result =
	 * DAESService.decryptSecureWireData(subscriberObData.getNiraResponse()); String
	 * s = new String(result.getResponse());
	 * 
	 * JsonNode jsonNode = objectMapper.readTree(s);
	 * System.out.println(" jsonNode :::: "+jsonNode); // Extract the "photo" field
	 * String photo = jsonNode.get("authenticPhoto").asText();
	 * onboardingData.setVerifierProvidedPhoto(photo); } else {
	 * onboardingData.setVerifierProvidedPhoto(obRequestDTO.getSubscriberData().
	 * getSubscriberSelfie()); }
	 * 
	 * // Set nira Response
	 * Optional.ofNullable(subscriberObData.getNiraResponse()).filter(response ->
	 * !response.isEmpty()) .ifPresent(onboardingData::setNiraResponse);
	 * 
	 * // set LOA based on onboarding method OnboardingMethod onboardingMethod =
	 * onBoardingMethodRepoIface
	 * .findByonboardingMethod(obRequestDTO.getOnboardingMethod());
	 * onboardingData.setLevelOfAssurance(onboardingMethod.getLevelOfAssurance());
	 * 
	 * raData.setCommonName( subscriberObData.getSecondaryIdentifier() + " " +
	 * subscriberObData.getPrimaryIdentifier());
	 * raData.setCertificateType(Constant.BOTH);
	 * raData.setCountryName(subscriberObData.getNationality());
	 * raData.setCreatedDate(AppUtil.getDate()); raData.setPkiPassword(
	 * subscriberObData.getSecondaryIdentifier() + " " +
	 * subscriberObData.getPrimaryIdentifier());
	 * raData.setPkiPasswordHash(subscriberObData.getSecondaryIdentifier() + " " +
	 * subscriberObData.getPrimaryIdentifier().hashCode()); raData.setPkiUserName(
	 * subscriberObData.getSecondaryIdentifier() + " " +
	 * subscriberObData.getPrimaryIdentifier());
	 * raData.setPkiUserNameHash(subscriberObData.getSecondaryIdentifier() + " " +
	 * subscriberObData.getPrimaryIdentifier().hashCode());
	 * raData.setSubscriberUid(obRequestDTO.getSuID());
	 * 
	 * issueCertDTO.setSubscriberUniqueId(obRequestDTO.getSuID());
	 * 
	 * FaceFeaturesDto faceFeaturesDto = new FaceFeaturesDto();
	 * faceFeaturesDto.setSubscriberPhoto(obRequestDTO.getSubscriberData().
	 * getSubscriberSelfie()); Selfie selfie = new Selfie();
	 * 
	 * // Save Nira response photo in place of selfie //
	 * if(subscriberObData.getNiraResponse() != null &&
	 * !subscriberObData.getNiraResponse().isEmpty()) { // // Result result =
	 * DAESService.decryptSecureWireData(subscriberObData.getNiraResponse()); //
	 * String niraSelfie = new String(result.getResponse()); //
	 * selfie.setSubscriberSelfie(niraSelfie); // }else {
	 * 
	 * // Remove BackGround image if(removeBackGroundFromImageBoolean){ ApiResponse
	 * apiResponseGetImage = getImageWithOutBackGround(
	 * obRequestDTO.getSubscriberData().getDocumentNumber(),
	 * obRequestDTO.getSubscriberData().getSubscriberSelfie());
	 * 
	 * if (apiResponseGetImage.getResult() == null) {
	 * selfie.setSubscriberSelfie(obRequestDTO.getSubscriberData().
	 * getSubscriberSelfie()); } else {
	 * selfie.setSubscriberSelfie(apiResponseGetImage.getResult().toString()); }
	 * }else { selfie.setSubscriberSelfie(obRequestDTO.getSubscriberData().
	 * getSubscriberSelfie()); }
	 * 
	 * // selfie.setSubscriberUniqueId(obRequestDTO.getSuID());
	 * CompletableFuture<ApiResponse> selfieResponse =
	 * edmsService.saveFileToEdms(selfie, "selfie", null); ApiResponse apiResponse =
	 * selfieResponse.get(); if (apiResponse.isSuccess()) { logger.info(CLASS +
	 * " addSubscriberObData res for saveFileToEdms: ", selfieResponse); String
	 * selfieURI = (String) apiResponse.getResult();
	 * onboardingData.setSelfieUri(selfieURI); } else { logger.info(CLASS +
	 * "addSubscriberObData res in false for saveFileToEdms: ",
	 * apiResponse.getMessage()); return
	 * exceptionHandlerUtil.createFailedResponseWithCustomMessage(apiResponse.
	 * getMessage(), null);
	 * 
	 * }
	 * 
	 * CompletableFuture<ApiResponse> selfieThumbnailResponse =
	 * edmsService.createThumbnailOfSelfie(selfie); ApiResponse selfieApiResponse =
	 * selfieThumbnailResponse.get(); if (selfieApiResponse.isSuccess()) {
	 * logger.info(CLASS + "addSubscriberObData res for createThumlbnailOfSelfie: ",
	 * selfieApiResponse.isSuccess());
	 * onboardingData.setSelfieThumbnailUri(selfieApiResponse.getResult().toString()
	 * ); } else { return
	 * exceptionHandlerUtil.createFailedResponseWithCustomMessage(selfieApiResponse.
	 * getMessage(), null);
	 * 
	 * }
	 * 
	 * savedSubscriber = subscriberRepoIface.save(subscriber);
	 * 
	 * additionalFile = subscriberObData; additionalFile.setSubscriberSelfie(null);
	 * additionalFile.setSubscriberUniqueId(onboardingData.getSubscriberUid());
	 * String additionalFieldSaved =
	 * objectMapper.writeValueAsString(additionalFile);
	 * onboardingData.setOnboardingDataFieldsJson(additionalFieldSaved);
	 * onboardingData.setRemarks(obRequestDTO.getSubscriberData().getRemarks());
	 * 
	 * faceFeaturesDto.setSuid(onboardingData.getSubscriberUid());
	 * faceFeaturesDto.setSubscriberName(savedSubscriber.getFullName());
	 * faceFeaturesDto.setSubscriberDataJson(additionalFieldSaved); // for
	 * faceFeatures rest call //
	 * saveFaceFeaturesAsync(faceFeaturesDto,registerFaceURL, restTemplate);
	 * 
	 * try { ExecutorService executor1 = Executors.newFixedThreadPool(1000);
	 * Runnable registerFaceWorkerThread = new
	 * RegisterFaceWorkerThread(registerFaceURL, faceFeaturesDto);
	 * executor1.execute(registerFaceWorkerThread); executor1.shutdown(); } catch
	 * (Exception e) { }
	 * 
	 * onboardingData = onboardingDataRepoIface.save(onboardingData); status =
	 * statusRepoIface.findBysubscriberUid(onboardingData.getSubscriberUid());
	 * 
	 * String subStatus =
	 * subscriberRepoIface.getSubscriberStatus(onboardingData.getSubscriberUid());
	 * logger.info("{}{} - getSubscriberStatus: {}", CLASS, Utility.getMethodName(),
	 * subStatus); if (subStatus != null) { if (subStatus.equals(Constant.ACTIVE)) {
	 * status.setSubscriberStatus(Constant.ACTIVE);
	 * status.setSubscriberStatusDescription(Constant.LOA_UPDATED);
	 * status.setUpdatedDate(AppUtil.getDate()); status =
	 * statusRepoIface.save(status); } else if
	 * (subStatus.equals(Constant.PIN_SET_REQUIRED)) {
	 * status.setSubscriberStatus(Constant.PIN_SET_REQUIRED);
	 * status.setSubscriberStatusDescription(Constant.LOA_UPDATED);
	 * status.setUpdatedDate(AppUtil.getDate()); status =
	 * statusRepoIface.save(status); } else {
	 * status.setSubscriberStatus(Constant.ONBOARDED);
	 * status.setSubscriberStatusDescription(Constant.ONBOARDED_SUCESSFULLY);
	 * status.setUpdatedDate(AppUtil.getDate()); status =
	 * statusRepoIface.save(status); raData = raRepoIface.save(raData); } } else {
	 * status.setSubscriberStatus(Constant.ONBOARDED);
	 * status.setSubscriberStatusDescription(Constant.ONBOARDED_SUCESSFULLY);
	 * status.setUpdatedDate(AppUtil.getDate()); status =
	 * statusRepoIface.save(status); raData = raRepoIface.save(raData); }
	 * 
	 * Subscriber s =
	 * subscriberRepoIface.findBysubscriberUid(onboardingData.getSubscriberUid());
	 * if (s != null) { try { ExecutorService executor =
	 * Executors.newFixedThreadPool(1000); Runnable visitorWorkerThread = new
	 * GenerateVisitorCardWorkerThread(visitorCardUrl, s, raData, onboardingData);
	 * executor.execute(visitorWorkerThread); executor.shutdown(); } catch
	 * (Exception e) { e.printStackTrace(); }
	 * 
	 * Date endTime = new Date(); double toatlTime =
	 * AppUtil.getDifferenceInSeconds(startTime, endTime);
	 * logModelServiceImpl.setLogModel(true, s.getSubscriberUid(),
	 * onboardingData.getGeolocation(), Constant.SUBSCRIBER_ONBOARDED,
	 * s.getSubscriberUid(), String.valueOf(toatlTime), startTime, endTime, null);
	 * logger.info("{}{} - Subscriber OnBoarding Data Saved: {}", CLASS,
	 * Utility.getMethodName(), s); return exceptionHandlerUtil
	 * .createSuccessResponse(
	 * "api.response.ugpass.application.submitted.successfully", s); } else {
	 * logModelServiceImpl.setLogModel(false, s.getSubscriberUid(), null,
	 * Constant.SUBSCRIBER_ONBOARDED, s.getSubscriberUid(), null, null, null, null);
	 * return exceptionHandlerUtil .createErrorResponseWithResult(
	 * "api.error.ugpass.application.submission.failed", s); } } catch (Exception e)
	 * { e.printStackTrace();
	 * logger.error("{}{} - Subscriber OnBoarding Data Exception: {}", CLASS,
	 * Utility.getMethodName(), e); sentryClientExceptions.captureExceptions(e);
	 * return
	 * onBoardingServiceException.handleExceptionWithStaticMessageWithSentry(e,
	 * obRequestDTO.getSuID()); } }
	 */

	@Async
	public void saveFaceFeaturesAsync(FaceFeaturesDto faceFeaturesDto, String faceUrl, RestTemplate restTemplate) {
		try {
			logger.info(CLASS + " Calling Face Features Save URL " + faceFeaturesDto.toString());
			// Set up headers and request entity
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<Object> reqEntity = new HttpEntity<>(faceFeaturesDto, headers);
			// Send POST request asynchronously
			ResponseEntity<ApiResponse> res = restTemplate.exchange(faceUrl, HttpMethod.POST, reqEntity,
					ApiResponse.class);
			// Return the result in a CompletableFuture
			logger.info("saveFaceFeaturesAsync res: " + res);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + " Error in saveFaceFeaturesAsync: " + e.getMessage());
		}
	}

	public ApiResponse getImageWithOutBackGround(String idDocNumber, String selfie) {
		String boarderControlImage;
		System.out.println(" Fetch Border Controll image getImageWithOutBackGround doc number" + idDocNumber);
		String image = subscriberRepoIface.getSimulatedBoarderControlImage(idDocNumber);
		try {
			// System.out.println(" getImageWithOutBackGround ");
			String jsonString;
			if (selfie != null || !selfie.isEmpty()) { // old if(image == null)
				jsonString = createJsonString(selfie);
			} else {
				jsonString = createJsonString(image);
			}
			HttpHeaders headers2 = new HttpHeaders();
			headers2.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<Object> reqEntity2 = new HttpEntity<>(jsonString, headers2);
			ResponseEntity<ApiResponse> res2 = restTemplate.exchange(removeBackGroundFromImageURL, HttpMethod.POST,
					reqEntity2, ApiResponse.class);
			if (res2.getStatusCodeValue() == 200 || res2.getStatusCodeValue() == 201) {
				boarderControlImage = res2.getBody().getResult().toString();
			} else {
				boarderControlImage = image;
			}
		} catch (Exception e) {
			System.out.println(" getImageWithOutBackGround ");
			e.printStackTrace();
			boarderControlImage = image;
		}
		return AppUtil.createApiResponse(true, "fetch image without background", boarderControlImage);
	}

	public static String createJsonString(String image) {
		// Use string concatenation to build the JSON
		// return "{"+"image='" + image.trim() +'\''+'}';
		return "{\n" + "\"image\": \"" + image + "\"\n" + "}";
	}

	@SuppressWarnings("unused")
	@Override
	public ApiResponse getSubscriberObData(HttpServletRequest httpServletRequest,
			GetSubscriberObDataDTO subscriberUID) {
		logger.info(CLASS + "getSubscriberObData req {}", subscriberUID);

		JsonNode root = null;// objectMapper.readTree(subscriberObData.getNiraResponse());

		// JsonNode dataNode = root.path("customerDetails").path("result").path("data");
		// JsonNode dataNode = root.path("customerDetails").path("Result").path("Data");
		try {
			String result = ValidationUtil.validate(subscriberUID);
			if (result != null) {
				System.out.println("Validation errors getSubscriberObData: " + result);
				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(result, null);

			}
			Subscriber subscriber = new Subscriber();
			String certStatus = null;
			SubscriberObData onboardingData = new SubscriberObData();
			SubscriberOnboardingData data = new SubscriberOnboardingData();
			List<SubscriberOnboardingData> dataList = new LinkedList<>();
			SubscriberObRequestDTO obRequestDTO = new SubscriberObRequestDTO();
			CertificateDetailDto certificateDetailDto = new CertificateDetailDto();
			SubscriberCertificate subscriberCertificate = new SubscriberCertificate();
			SubscriberStatus status = new SubscriberStatus();

			List<String> paymentStatus = subscriberRepoIface.subscriberPaymnetStatus(subscriberUID.getSuid());
			logger.info(CLASS + "subscriberPaymnetStatus paymentStatus {}", paymentStatus);
			List<String> statuss = subscriberRepoIface.subscriberPaymnetInitaiatedStatus(subscriberUID.getSuid());
			String paymentIntiatiedStatus = statuss.isEmpty() ? null : statuss.get(0); // latest

			logger.info(CLASS + " subscriberPaymnetInitaiatedStatus paymentIntiatiedStatus ", paymentIntiatiedStatus);
			List<String> statuses = subscriberRepoIface.subscriberPaymnetCertStatus(subscriberUID.getSuid());
			String paymentCertStatus = statuses.isEmpty() ? null : statuses.get(0); // latest record

			logger.info(CLASS + "subscriberPaymnetCertStatus paymentCertStatus {}", paymentCertStatus);
			if (subscriberUID != null) {
				subscriber = subscriberRepoIface.findBysubscriberUid(subscriberUID.getSuid());
				if (subscriber == null) {
					return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.details.not.found");
				}

				/*
				 * update subscriber app version when app version is greater than current
				 * version
				 */
				try {
					ExecutorService executor = Executors.newFixedThreadPool(20);
					Runnable visitorWorkerThread = new VersionComparatorThread(subscriber, subscriberRepoIface,
							httpServletRequest);
					executor.execute(visitorWorkerThread);
					executor.shutdown();
				} catch (Exception e) {
					e.printStackTrace();
				}

				List<String> list = subscriberRepoIface.getCertStatus(subscriberUID.getSuid());
				certStatus = list.isEmpty() ? null : list.get(0);

//                List<String> list =
//                        subscriberRepoIface.getCertStatus(subscriberUID.getSuid());
//
//                SubscriberCertificateLifeCycle latest = list.isEmpty() ? null : list.get(0);
//
//                String certStatus1 = (latest != null) ? latest.getCertificateStatus() : null;

				// certStatus = subscriberRepoIface.getCertStatus(subscriberUID.getSuid());
//

				dataList = onboardingDataRepoIface.getBySubUid(subscriberUID.getSuid());

				List<SubscriberCertificate> certificates = subscriberCertificatesRepoIface
						.findBySubscriberUniqueId(subscriberUID.getSuid());
				SubscriberCertificate subscriberCertificates = certificates.isEmpty() ? null : certificates.get(0); // latest
				System.out.println("randommmm" + subscriberCertificates);

				if (subscriberCertificates != null) {
					String expDate = null;
					if (subscriberCertificates.getCerificateExpiryDate() != null) {
						expDate = subscriberCertificates.getCerificateExpiryDate().toString().split(" ")[0]; // keep
																												// only
																												// the
																												// date
																												// part
					}

					String issueDate = null;
					if (subscriberCertificates.getCertificateIssueDate() != null) {
						issueDate = subscriberCertificates.getCertificateIssueDate().toString().split(" ")[0];
					}
					certificateDetailDto.setCertStatus(subscriberCertificates.getCertificateStatus() != null
							? subscriberCertificates.getCertificateStatus().toString()
							: null);

					certificateDetailDto.setIssueDate(issueDate);
					certificateDetailDto.setExpiryDate(expDate);

					if (subscriberCertificate.getUpdatedDate() != null) {
						String[] revokeDate = subscriberCertificate.getUpdatedDate().toString().split(" ");
						certificateDetailDto.setRevokeDate(revokeDate[0]);
					}
					obRequestDTO.setCertificateDetailDto(certificateDetailDto);
				}

				if (!dataList.isEmpty()) {
					if (dataList.size() > 1) {
						data = findLatestOnboardedSub(dataList);
					} else {
						data = dataList.get(0);
					}
				}

				status = statusRepoIface.findBysubscriberUid(subscriberUID.getSuid());
				if (data != null) {

					root = objectMapper.readTree(data.getNiraResponse()); // data.getNiraResponse();

					JsonNode dataNode = root.path("customerDetails").path("Result").path("Data");

					obRequestDTO.setResidenceInfo(dataNode.path("ResidenceInfo"));
					obRequestDTO.setActivePassport(dataNode.path("ActivePassport"));

					onboardingData.setDateOfBirth(subscriber.getDateOfBirth());
					onboardingData.setDocumentCode(data.getIdDocCode());
					onboardingData.setDocumentNumber(data.getIdDocNumber());
					onboardingData.setDocumentType(data.getIdDocType());
					onboardingData.setSubscriberUniqueId(data.getSubscriberUid());

					ObjectMapper mapper = new ObjectMapper();
					onboardingData = mapper.readValue(data.getOnboardingDataFieldsJson(), SubscriberObData.class);
					onboardingData.setNationality(onboardingData.getNationality());
					if (subscriberUID.isSelfieRequired()) {

						if (isOnboardingFee) {
							// UgPASS always isOnboardingFee true
							ApiResponse response = getSubscriberSelfie(data.getSelfieUri());
							if (boarderControllPhotoRequired) {
								if (data.getVerifierProvidedPhoto() != null || data.getVerifierProvidedPhoto() != "") {
									System.out.println("Inside isSelfieRequired boarderControlPhoto is not null");
									obRequestDTO.setBoarderControlPhoto(data.getVerifierProvidedPhoto());
								}
							} else {
								if (response.isSuccess()) {
									// onboardingData.setSubscriberSelfie((String) response.getResult());
									obRequestDTO.setBoarderControlPhoto((String) response.getResult());
								}
							}
							if (response.isSuccess()) {
								onboardingData.setSubscriberSelfie((String) response.getResult());
							} else {
								onboardingData.setSubscriberSelfie("");
							}
						} else {
							obRequestDTO.setBoarderControlPhoto(data.getSelfie());
							onboardingData.setSubscriberSelfie(data.getSelfie());
						}

					}

					// 3️⃣ FORCE expiry update when onboarding method is NIN
					if ("NIN".equalsIgnoreCase(data.getOnboardingMethod())) {
						System.out.println(" data.getOnboardingMethod() ::" + data.getOnboardingMethod());
						LocalDateTime expiry = LocalDateTime.now().plusYears(1).toLocalDate().atStartOfDay();

						DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

						onboardingData.setDateOfExpiry(expiry.format(formatter));
					}
					if (!isOnboardingFee) {
						String uaeKycId = "uaeKycId";
						// onboardingData.setNiraResponse("{"+uaeKycId+":"+ data.getUaeKycId()+"}");
						onboardingData.setNiraResponse("{\"uaeKycId\":\"" + data.getUaeKycId() + "\"}");

						obRequestDTO.setPassportNumber(subscriber.getPassportNumber());
						obRequestDTO.setEmiratesIdNumber(subscriber.getNationalIdNumber());
						obRequestDTO.setEmiratesIdDocumentNumber(subscriber.getNationalIdCardNumber());
					}

					obRequestDTO.setSubscriberData(onboardingData);
					obRequestDTO.setSubscriberType(data.getSubscriberType());
					obRequestDTO.setConsentId(1);
					obRequestDTO.setSuID(data.getSubscriberUid());
					obRequestDTO.setOnboardingMethod(data.getOnboardingMethod());
					obRequestDTO.setLevelOfAssurance(data.getLevelOfAssurance());
					obRequestDTO.setTemplateId(data.getTemplateId());
					obRequestDTO.setOnboardingApprovalStatus(status.getSubscriberStatus());

					if (subscriber.getMobileNumber() != null) {
						obRequestDTO.setMobileNo(subscriber.getMobileNumber());
					}
					if (subscriber.getEmailId() != null) {
						obRequestDTO.setEmailId(subscriber.getEmailId());
					}

					if (subscriber.getTitle() != null) {
						obRequestDTO.setTitle(subscriber.getTitle());
					}

					if (!paymentStatus.isEmpty()) {
						obRequestDTO.setOnboardingPaymentStatus(paymentStatus.get(0));
					} else if (paymentIntiatiedStatus != null) {
						obRequestDTO.setOnboardingPaymentStatus(Constant.PAYMENT_STATUS_INITIATED);
					} else {
						obRequestDTO.setOnboardingPaymentStatus(Constant.PAYMENT_STATUS_PENDING);
					}

					if (certStatus == null) {
						obRequestDTO.setCertStatus(Constant.PENDING);
					} else if (certStatus.equalsIgnoreCase(Constant.FAIL) || certStatus.equals(Constant.FAILED)) {
						obRequestDTO.setCertStatus(Constant.FAILED);
					} else if (certStatus.equalsIgnoreCase(Constant.CERT_REVOKED)
							|| certStatus.equals(Constant.REVOKED)) {
//						obRequestDTO.setCertStatus("REVOKED");
//						obRequestDTO.setOnboardingPaymentStatus("Pending");
						if (paymentCertStatus == null) {
							obRequestDTO.setCertStatus(Constant.REVOKED);
							obRequestDTO.setOnboardingPaymentStatus(Constant.PAYMENT_STATUS_PENDING);
						} else if (paymentCertStatus.equalsIgnoreCase(Constant.SUCCESS)) {
							obRequestDTO.setCertStatus(Constant.REVOKED);
							obRequestDTO.setOnboardingPaymentStatus(Constant.PAYMENT_STATUS_SUCCESS);
						} else if (paymentCertStatus.equalsIgnoreCase(Constant.FAILED)) {
							obRequestDTO.setCertStatus(Constant.REVOKED);
							obRequestDTO.setOnboardingPaymentStatus(Constant.PAYMENT_STATUS_FAILED);
						} else {
							obRequestDTO.setCertStatus(Constant.REVOKED);
							obRequestDTO.setOnboardingPaymentStatus(Constant.PAYMENT_STATUS_INITIATED);
						}
					} else if (certStatus.equalsIgnoreCase(Constant.CERT_EXPIRED)
							|| certStatus.equals(Constant.EXPIRED)) {
						// obRequestDTO.setCertStatus(Constant.EXPIRED);
						// obRequestDTO.setOnboardingPaymentStatus(Constant.PAYMENT_STATUS_PENDING);
						if (paymentCertStatus == null) {
							obRequestDTO.setCertStatus(Constant.EXPIRED);
							obRequestDTO.setOnboardingPaymentStatus(Constant.PAYMENT_STATUS_PENDING);
						} else if (paymentCertStatus.equalsIgnoreCase(Constant.SUCCESS)) {
							obRequestDTO.setCertStatus(Constant.EXPIRED);
							obRequestDTO.setOnboardingPaymentStatus(Constant.PAYMENT_STATUS_SUCCESS);
						} else if (paymentCertStatus.equalsIgnoreCase(Constant.FAILED)) {
							obRequestDTO.setCertStatus(Constant.EXPIRED);
							obRequestDTO.setOnboardingPaymentStatus(Constant.PAYMENT_STATUS_FAILED);
						} else {
							obRequestDTO.setCertStatus(Constant.REVOKED);
							obRequestDTO.setOnboardingPaymentStatus(Constant.PAYMENT_STATUS_INITIATED);
						}
					} else {
						obRequestDTO.setCertStatus(certStatus);
					}

					if (priAuthSchemeBoolean) {
						TotpDto totpDto = new TotpDto();
						totpDto.setSuid(subscriber.getSubscriberUid());
						totpDto.setFullName(subscriber.getFullName());
						totpDto.setPriAuthScheme(priAuthScheme);
						ApiResponse totpApiResponse = getTotp(totpDto);

						String totpResp = objectMapper.writerWithDefaultPrettyPrinter()
								.writeValueAsString(totpApiResponse.getResult());
						TotpDtoResp totpDtoResp = objectMapper.readValue(totpResp, TotpDtoResp.class);
						if (totpDtoResp == null) {
							return exceptionHandlerUtil.createErrorResponse(
									"api.error.authentication.data.should.not.be.null.or.empty.subscriber.onBoarding.data.not.saved");
						}
						obRequestDTO.setTotpResp(totpDtoResp);
					}

					return exceptionHandlerUtil.createSuccessResponse("api.response.subscriber.onboarding.data",
							obRequestDTO);
				} else {
					return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.id.not.matched");
				}
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.id.cant.be.empty");
			}
		} catch (Exception e) {
			logger.error(CLASS + "getSubscriberObData Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	@Override
	public ApiResponse getVerificationChannelResponse(HttpServletRequest request, String subscriberUID) {
		try {
			logger.info(" getVerificationChannelResponse suid ::" + subscriberUID);
			JsonNode root = null;
			SubscriberOnboardingData subscriberOnboardingData = new SubscriberOnboardingData();
			List<SubscriberOnboardingData> subscriberOnboardingDataList = onboardingDataRepoIface
					.getBySubUid(subscriberUID);
			if (!subscriberOnboardingDataList.isEmpty()) {
				if (subscriberOnboardingDataList.size() > 1) {
					subscriberOnboardingData = findLatestOnboardedSub(subscriberOnboardingDataList);
				} else {
					subscriberOnboardingData = subscriberOnboardingDataList.get(0);
				}
			}

			if (subscriberOnboardingData != null) {
				root = objectMapper.readTree(subscriberOnboardingData.getNiraResponse());
				// JsonNode dataNode = root.path("customerDetails").path("Result").path("Data");
				ObjectNode dataNode = (ObjectNode) root.path("customerDetails").path("Result").path("Data");
				// Set fields to explicit JSON null
				dataNode.putNull("ImmigrationFile");
				dataNode.putNull("TravelDetail");
				dataNode.putNull("Documents");
				return exceptionHandlerUtil.createSuccessResponse("api.response.verification.channel.response",
						dataNode);
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.no.data.found");
			}
		} catch (Exception e) {
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	public void setLogModel(Boolean response, Subscriber subscriber, String geoLocation)
			throws ParseException, PKICoreServiceException {
		logger.info(CLASS + "Set LogModel {} and subscriber {} and geoLocation {}", response, subscriber, geoLocation);
		LogModelDTO logModel = new LogModelDTO();
		logModel.setIdentifier(subscriber.getSubscriberUid());
		logModel.setCorrelationID(generateSubscriberUniqueId());
		logModel.setTransactionID(generateSubscriberUniqueId());
		logModel.setTimestamp(null);
		logModel.setStartTime(getTimeStampString());
		logModel.setEndTime(getTimeStampString());
		logModel.setServiceName(ServiceNames.SUBSCRIBER_ONBOARDED.toString());
		logModel.setLogMessage("RESPONSE");
		logModel.setTransactionType(TransactionType.BUSINESS.toString());
		logModel.setGeoLocation(geoLocation);
		logModel.seteSealUsed(false);
		logModel.setSignatureType(null);

		if (response) {
			logModel.setLogMessageType(LogMessageType.SUCCESS.toString());
		} else {
			logModel.setLogMessageType(LogMessageType.FAILURE.toString());
		}
		logModel.setChecksum(null);

		try {

			ObjectMapper objectMapper = new ObjectMapper();
			String json = objectMapper.writeValueAsString(logModel);
			System.out.println("json => " + json);
			Result checksumResult = DAESService.addChecksumToTransaction(json);
			String push = new String(checksumResult.getResponse());
			LogModelDTO log = objectMapper.readValue(push, LogModelDTO.class);
			mqSender.send(log);
		} catch (Exception e) {
			logger.error("Set LogModel Exception {}", e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public ApiResponse resetPin(GetSubscriberObDataDTO subscriberObDataDTO) {
		logger.info(CLASS + " Reset Pin request {}", subscriberObDataDTO);
		SubscriberOnboardingData onboardingData = new SubscriberOnboardingData();
		ResetPinDTO pinDTO = new ResetPinDTO();

		String result = ValidationUtil.validate(subscriberObDataDTO);
		if (result != null) {
			System.out.println("Validation errors: " + result);
			// throw new GetSubscriberObDataDTO(result);
			return exceptionHandlerUtil.createFailedResponseWithCustomMessage(result, null);
		}
		ApiResponse response = null;
		try {
			List<SubscriberOnboardingData> onboardingDataList = onboardingDataRepoIface
					.getBySubUid(subscriberObDataDTO.getSuid());
			if (onboardingDataList != null) {
				if (onboardingDataList.size() > 1) {
					onboardingData = findLatestOnboardedSub(onboardingDataList);
				} else {
					onboardingData = onboardingDataList.get(0);
				}
			}
			if (onboardingData != null) {
				pinDTO.setIdDocNumber(onboardingData.getIdDocNumber());
				if (subscriberObDataDTO.isSelfieRequired()) {
					if (isOnboardingFee) {
						pinDTO.setSelfie(onboardingData.getSelfie());
					} else {
						response = getSubscriberSelfie(onboardingData.getSelfieUri());
						if (response.isSuccess()) {
							pinDTO.setSelfie((String) response.getResult());
						}
					}
				} else {
					pinDTO.setSelfie(null);
				}

				return exceptionHandlerUtil.createSuccessResponse("api.response.reset.pin.data", pinDTO);
//				return AppUtil.createApiResponse(true,
//						"api.response.reset.pin.data", pinDTO);
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.no.data.found");
//				return AppUtil.createApiResponse(false,
//						"api.error.no.data.found", null);
			}
		} catch (Exception e) {
			logger.error(CLASS + "resetPin Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	@Override
	public ApiResponse getSubscriberSelfie(String uri) {
		logger.info(CLASS + " getBase64String uri {}", uri);
		try {
			HttpHeaders headersForGet = new HttpHeaders();
			HttpEntity<Object> requestEntityForGet = new HttpEntity<>(headersForGet);
			ResponseEntity<Resource> downloadUrlResult = restTemplate.exchange(uri, HttpMethod.GET, requestEntityForGet,
					Resource.class);
			byte[] buffer = IOUtils.toByteArray(downloadUrlResult.getBody().getInputStream());
			String image2 = new String(Base64.getEncoder().encode(buffer));
			return exceptionHandlerUtil.createSuccessResponse("api.response.base64.of.image.fetched.successfully",
					image2);
		} catch (Exception e) {
			logger.error(CLASS + "getBase64String Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	@Override
	public ResponseEntity<Object> getVideoLiveStreaming(String subscriberUid) {
		logger.info(CLASS + "getVideoLiveStreaming subscriberUid {}", subscriberUid);
		try {

			if (subscriberUid == null || subscriberUid.isEmpty()) {
				return ResponseEntity.status(HttpStatus.NOT_FOUND)
						.body(exceptionHandlerUtil.createErrorResponse("api.error.subscriberuid.cant.be.null"));
			}

			if (!(Objects.isNull(subscriberUid) || subscriberUid.isEmpty())) {
				String url = subscriberRepoIface.getSubscriberUid(subscriberUid);
				if (!Objects.isNull(url)) {
					HttpHeaders headersForGet = new HttpHeaders();
					HttpEntity<Object> requestEntityForGet = new HttpEntity<>(headersForGet);
					ResponseEntity<Resource> downloadUrlResult = restTemplate.exchange(url, HttpMethod.GET,
							requestEntityForGet, Resource.class);

					return ResponseEntity.status(HttpStatus.OK).header("Content-Type", "video/mp4")
							.body(downloadUrlResult.getBody());
				} else {
					logger.info(CLASS + "getVideoLiveStreaming No video found {}", HttpStatus.NOT_FOUND);
					return ResponseEntity.status(HttpStatus.NOT_FOUND)
							.body(exceptionHandlerUtil.createErrorResponse("api.error.no.video.found"));

				}

			} else {
				logger.info(CLASS + "getVideoLiveStreaming Subscriber not found {}", HttpStatus.NOT_FOUND);
				return ResponseEntity.status(HttpStatus.NOT_FOUND)
						.body(exceptionHandlerUtil.createErrorResponse("api.error.subscriber.not.found"));

			}
		} catch (Exception e) {
			logger.error(CLASS + "getVideoLiveStreaming Exception {}", e.getMessage());
			e.printStackTrace();
			// return exceptionHandlerUtil.handleHttpException(e);
			return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED)
					.body(exceptionHandlerUtil.handleHttpException(e));

		}

	}

	public static SubscriberOnboardingData findLatestOnboardedSub(
			List<SubscriberOnboardingData> subscriberOnboardingData) {
		Date[] dates = new Date[subscriberOnboardingData.size()];

		int i = 0;
		SimpleDateFormat simpleDateFormat = null;
		for (SubscriberOnboardingData s : subscriberOnboardingData) {

			try {
				simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				Date date = simpleDateFormat.parse(s.getCreatedDate());

				dates[i] = date;
				i++;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		Date latestDate = getLatestDate(dates);
		String latestDateString = simpleDateFormat.format(latestDate);
		for (SubscriberOnboardingData s : subscriberOnboardingData) {
			if (s.getCreatedDate().equals(latestDateString)) {
				return s;
			}
		}
		return null;
	}

	public static Date getLatestDate(Date[] dates) {
		Date latestDate = null;
		if ((dates != null) && (dates.length > 0)) {
			for (Date date : dates) {
				if (date != null) {
					if (latestDate == null) {
						latestDate = date;
					}
					latestDate = date.after(latestDate) ? date : latestDate;
				}
			}
		}
		return latestDate;
	}

	private String getTimeStampString() throws ParseException {
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		return f.format(new Date());
	}

	@Override
	public ResponseEntity<Object> getVideoLiveStreamingLocalEdms(String subscriberUid) {
		try {

			if (subscriberUid == null || subscriberUid.isEmpty()) {
				return ResponseEntity.status(HttpStatus.NOT_FOUND)
						.body(exceptionHandlerUtil.createErrorResponse("api.error.subscriberuid.cant.be.null"));
			}

			logger.info(CLASS + "getVideoLiveStreamingLocalEdms req subscriberUid {}", subscriberUid);
			if (!(Objects.isNull(subscriberUid) || subscriberUid.isEmpty())) {
				String url = livelinessRepository.getSubscriberUid(subscriberUid);
				if (!Objects.isNull(url)) {
					HttpHeaders headersForGet = new HttpHeaders();
					HttpEntity<Object> requestEntityForGet = new HttpEntity<>(headersForGet);
					ResponseEntity<Resource> downloadUrlResult = restTemplate.exchange(url, HttpMethod.GET,
							requestEntityForGet, Resource.class);

					return ResponseEntity.status(HttpStatus.OK).header("Content-Type", "video/mp4")
							.body(downloadUrlResult.getBody());
				} else {

					logger.error(CLASS + "getVideoLiveStreamingLocalEdms No video found {}", HttpStatus.NOT_FOUND);
					return ResponseEntity.status(HttpStatus.NOT_FOUND)
							.body(exceptionHandlerUtil.createErrorResponse("api.error.no.video.found"));
				}
			} else {
				logger.error(CLASS + "getVideoLiveStreamingLocalEdms Subscriber not found {}", HttpStatus.NOT_FOUND);
				return ResponseEntity.status(HttpStatus.NOT_FOUND)
						.body(exceptionHandlerUtil.createErrorResponse("api.error.subscriber.not.found"));

			}
		} catch (JDBCConnectionException | ConstraintViolationException | DataException | LockAcquisitionException
				| PessimisticLockException | QueryTimeoutException | SQLGrammarException | GenericJDBCException ex) {
			ex.printStackTrace();
			logger.error(CLASS + "saveSubscriberData Exception {}", ex.getMessage());
			return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(
					AppUtil.createApiResponse(false, "api.error.something.went.wrong.please.try.after.sometime", null));
		} catch (Exception e) {
			logger.error(CLASS + "getVideoLiveStreamingLocalEdms Exception {}", e.getMessage());
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).body(
					AppUtil.createApiResponse(false, "api.error.something.went.wrong.please.try.after.sometime", null));
		}
	}

	@Override
	public ApiResponse addTrustedUsers(TrustedUserDto emails) {
		try {
			List<String> emailsListDb = trustedUserRepoIface.getTrustedEmails();
			List<String> secondList = new ArrayList<String>();
			List<TrustedUser> saveTrustedUser = new ArrayList<TrustedUser>();
			logger.info(CLASS + "addTrustedUsers emailsListDb {}", emailsListDb);
			logger.info(CLASS + "addTrustedUsers secondList {}", secondList);
			if (Objects.nonNull(emails) && !CollectionUtils.isEmpty(emails.getEmails())) {
				if (!CollectionUtils.isEmpty(emailsListDb)) {
					for (TrustedEmails trustedEmails : emails.getEmails()) {
						secondList.add(trustedEmails.getEmail());
					}
					secondList.retainAll(emailsListDb);
					if (!secondList.isEmpty()) {
						return exceptionHandlerUtil
								.createErrorResponseWithResult("api.error.duplicate.emails.are.present", secondList);
					} else {

						for (TrustedEmails trustedEmails : emails.getEmails()) {
							saveTrustedUser.add(saveTrustedUsers(trustedEmails));
						}
						trustedUserRepoIface.saveAll(saveTrustedUser);
						return exceptionHandlerUtil.successResponse("api.response.list.save.successfully");

					}

				} else {
					return exceptionHandlerUtil.createErrorResponse("api.error.trusted.user.email.list.is.empty");
				}
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.trusted.user.email.list.is.empty");
			}

		} catch (Exception e) {
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	public TrustedUser saveTrustedUsers(TrustedEmails trustedEmails) {
		TrustedUser trustedUser = new TrustedUser();
		trustedUser.setEmailId(trustedEmails.getEmail());
		trustedUser.setFullName(trustedEmails.getName());
		trustedUser.setMobileNumber(trustedEmails.getMobileNo());
		trustedUser.setTrustedUserStatus(trustedUserStatus);
		return trustedUser;
	}

	@Override
	public ApiResponse getSubscriberDetailsReports(String startDate, String endDate) {
		try {
			logger.info(CLASS + "getSubscriberDetailsReport req startDate {} and endDate {}", startDate, endDate);
			if (startDate != null && endDate != null) {
				List<SubscriberCertificateDetails> completeDetail = subscriberCertificateDetailsRepoIface
						.getSubscriberReports(startDate, endDate);
				List<SubscriberReportsResponseDto> details = new ArrayList<>();

				if (Objects.nonNull(completeDetail) && !completeDetail.isEmpty()) {

					for (SubscriberCertificateDetails subscriberCompleteDetail : completeDetail) {
						SubscriberReportsResponseDto reportsResponseDto = new SubscriberReportsResponseDto();
						reportsResponseDto.setFullName(subscriberCompleteDetail.getFullName());
						reportsResponseDto.setIdDocNumber(subscriberCompleteDetail.getIdDocNumber());
						reportsResponseDto.setOnboardingMethod(subscriberCompleteDetail.getOnboardingMethod());
						reportsResponseDto
								.setCertificateSerialNumber(subscriberCompleteDetail.getCertificateSerialNumber());
						reportsResponseDto
								.setCertificateIssueDate(subscriberCompleteDetail.getCertificateIssueDate().toString());
						reportsResponseDto
								.setCerificateExpiryDate(subscriberCompleteDetail.getCerificateExpiryDate().toString());
						details.add(reportsResponseDto);
					}
					logger.info(
							CLASS + "getSubscriberDetailsReports Succssfully fetched subscriber certificate details {}",
							details);

					return exceptionHandlerUtil.createSuccessResponse(
							"api.error.successfully.fetched.subscriber.certificate.details", details);
				} else {
					logger.info(CLASS + " getSubscriberDetailsReports No Records Found");
					return exceptionHandlerUtil.createErrorResponse("api.response.no.records.found");

				}
			} else {
				logger.info(CLASS + "getSubscriberDetailsReports Date cant should be null or empty");
				return exceptionHandlerUtil.createErrorResponse("api.error.date.cant.should.be.null.or.empty");

			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + " getSubscriberDetailsReports Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	int isOptionData1Present(String optionalData1) {

		int optionalDataCount = onboardingDataRepoIface.getOptionalData1(optionalData1);

		return optionalDataCount;
	}

	@Override
	public ApiResponse updatePhoneNumber(UpdateDto updateDto) {
		try {
			logger.info(CLASS + " updatePhoneNumber Suid {}", updateDto.toString());
			if (updateDto.getSuid() == null || updateDto.getSuid().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriberuid.cant.be.null");

			}
			if (updateDto.getMobileNumber() == null || updateDto.getMobileNumber().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.mobile.number.cant.be.null.or.empty");
			}

			Date d1 = subscriberHistoryRepo.getLatestForMobile(updateDto.getSuid());
			logger.info(CLASS + " updatePhoneNumber latest date {} ", d1);
			if (d1 != null) {
				Date d2 = AppUtil.getCurrentDate();
				long difference_In_Time = d2.getTime() - d1.getTime();
				long difference_In_Days = TimeUnit.MILLISECONDS.toDays(difference_In_Time) % 365;
				System.out.println(difference_In_Days);
				if (difference_In_Days <= 30) {

					return exceptionHandlerUtil.createErrorResponse(
							"api.error.cant.change.the.phone.number.because.you.changed.it.recently");

				}
			}

			Subscriber sub = subscriberRepoIface.findBymobileNumber(updateDto.getMobileNumber());

			Subscriber subscriber = subscriberRepoIface.findBysubscriberUid(updateDto.getSuid());
			if (subscriber == null) {
				return AppUtil.createApiResponse(false, "api.error.subscriber.not.found", null);
			}
			if (subscriber.getMobileNumber().equals(updateDto.getMobileNumber())) {

				return exceptionHandlerUtil
						.createErrorResponse("api.error.your.old.number.and.entered.mobile.number.are.same");
			}
			if (sub != null) {

				return exceptionHandlerUtil.createErrorResponse("api.error.this.mobile.number.is.already.in.use");

			}
			// create new subHistory instance and save old records
			SubscriberContactHistory subscriberContactHistory = new SubscriberContactHistory();
			subscriberContactHistory.setSubscriberUid(subscriber.getSubscriberUid());
			subscriberContactHistory.setMobileNumber(subscriber.getMobileNumber());
			// subscriberContactHistory.setEmailId(subscriber.getEmailId());
			subscriberContactHistory.setCreatedDate(AppUtil.getCurrentDate());
			subscriberHistoryRepo.save(subscriberContactHistory);

			// update subscriber phone
			subscriber.setMobileNumber(updateDto.getMobileNumber());
			subscriberRepoIface.save(subscriber);

			return exceptionHandlerUtil.createSuccessResponse("api.error.phone.number.updated", subscriber);

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + " updatePhoneNumber Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	@Override
	public ApiResponse updateEmail(UpdateDto updateDto) {
		try {
			logger.error(CLASS + " updatePhoneNumber Suid {}", updateDto.getSuid());
			if (updateDto.getSuid() == null || updateDto.getSuid().isEmpty()) {

				return exceptionHandlerUtil.createErrorResponse("api.error.subscriberuid.cant.be.null");

			}
			if (updateDto.getEmail() == null || updateDto.getEmail().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.email.id.cant.be.empty");
			}
			Date d1 = subscriberHistoryRepo.getLatestForEmail(updateDto.getSuid());
			if (d1 != null) {
				Date d2 = AppUtil.getCurrentDate();
				long difference_In_Time = d2.getTime() - d1.getTime();
				long difference_In_Days = TimeUnit.MILLISECONDS.toDays(difference_In_Time) % 365;
				System.out.println(difference_In_Days);
				if (difference_In_Days <= 30) {

					return exceptionHandlerUtil
							.createErrorResponse("api.error.cant.change.the.email.because.you.changed.it.recently");
				}
			}

			Subscriber sub = subscriberRepoIface.findByemailId(updateDto.getEmail());

			Subscriber subscriber = subscriberRepoIface.findBysubscriberUid(updateDto.getSuid());
			if (subscriber == null) {

				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.not.found");

			}
			if (subscriber.getEmailId().equals(updateDto.getEmail())) {

				return exceptionHandlerUtil
						.createErrorResponse("api.error.your.old.email.and.entered.emailId.are.same");

			}
			// checking if entered mail is already in use with other subscriber
			if (sub != null) {

				return exceptionHandlerUtil.createErrorResponse("api.error.this.email.is.already.in.use");

			}

			int orgEmailCount = orgContactsEmailRepository.findByOrgEmailAndNotUgPassEmail(updateDto.getEmail(),
					subscriber.getEmailId());
			int orgMobileCount = orgContactsEmailRepository.findByOrgEmailAndNotMobile(updateDto.getEmail(),
					subscriber.getMobileNumber());
			int orgNinCount = orgContactsEmailRepository.findByOrgEmailAndNotNin(updateDto.getEmail(),
					subscriber.getIdDocNumber());
			int orgPassportCount = orgContactsEmailRepository.findByOrgEmailAndNotPassport(updateDto.getEmail(),
					subscriber.getIdDocNumber());
			if (orgEmailCount != 0 || orgPassportCount != 0 || orgMobileCount != 0 || orgNinCount != 0) {

				return exceptionHandlerUtil.createErrorResponse(
						"api.error.this.email.is.already.registered.with.another.organization.subscriber.email");

			}

			// create new subHistory instance and save old records
			SubscriberContactHistory subscriberContactHistory = new SubscriberContactHistory();
			subscriberContactHistory.setSubscriberUid(subscriber.getSubscriberUid());
			// subscriberContactHistory.setMobileNumber(subscriber.getMobileNumber());
			subscriberContactHistory.setEmailId(subscriber.getEmailId());
			subscriberContactHistory.setCreatedDate(AppUtil.getCurrentDate());
			subscriberHistoryRepo.save(subscriberContactHistory);

			// update subscriber email
			// subscriber.setMobileNumber(updateDto.getMobileNumber());
			subscriber.setEmailId(updateDto.getEmail());
			subscriberRepoIface.save(subscriber);

			return exceptionHandlerUtil.createSuccessResponse("api.response.email.updated", subscriber);

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + " updatePhoneNumber Exception ", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	@Override
	public ApiResponse sendOtpEmail(UpdateOtpDto otpDto) {
		try {
			if (otpDto.getEmail() == null || otpDto.getEmail().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.email.id.cant.be.empty");
//				return AppUtil.createApiResponse(false,
//						"api.error.email.id.cant.be.empty", null);
			}
			OTPResponseDTO otpResponse = new OTPResponseDTO();

			if (otpDto.getEmail().equals(testAndroidEmail) || otpDto.getEmail().equals(testIosEmail)) {
				ApiResponse apiResponseDemo = verifyOtp(null, otpDto.getEmail());
				if (apiResponseDemo.isSuccess()) {
//					otpResponse.setMobileOTP(null);
//					otpResponse.setEmailOTP(null);
//					otpResponse.setTtl(timeToLive);
//					otpResponse.setMobileEncrptyOTP(encryptedString(mobileOTP));
					return exceptionHandlerUtil.createSuccessResponse("api.response.ok", apiResponseDemo.getResult());
//					return AppUtil.createApiResponse(true,
//							"api.response.ok",
//							apiResponseDemo.getResult());
				}
			}

			String emailOTP = generateOtp(6);
			System.out.println("emailOTP >> " + emailOTP + " : " + AppUtil.encrypt(emailOTP));
			EmailReqDto dto = new EmailReqDto();
			dto.setEmailOtp(emailOTP);
			dto.setEmailId(otpDto.getEmail());
			dto.setTtl(timeToLive);

			ApiResponse res = sendEmailToSubscriber(dto);

			otpResponse.setMobileOTP(null);
			otpResponse.setEmailOTP(null);
			otpResponse.setTtl(timeToLive);
			otpResponse.setEmailEncrptyOTP(encryptedString(emailOTP));

			if (res.isSuccess()) {
				System.out.println("email res >> " + res.getMessage());
				System.out.println("Email Sent Successfully");
				return exceptionHandlerUtil.createSuccessResponse("api.response.ok", otpResponse);
			} else {
				System.out.println("IN Email Excption >> " + res);
				return exceptionHandlerUtil
						.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");
			}
		} catch (Exception e) {
			e.printStackTrace();
			sentryClientExceptions.captureExceptions(e);
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	public ApiResponse verifyOtp(String mobNo, String email) {
		ApiResponse apiResponse = new ApiResponse();

		OTPResponseDTO otpResponse = new OTPResponseDTO();

		if (email != null) {
			otpResponse.setEmailEncrptyOTP(AppUtil.encryptedString("12345"));
		} else {
			otpResponse.setMobileEncrptyOTP(AppUtil.encryptedString("123456"));
		}
		otpResponse.setTtl(180);
		apiResponse.setMessage("Otp verfication done");
		apiResponse.setSuccess(true);
		apiResponse.setResult(otpResponse);
		return apiResponse;

	}

	@Override
	public ApiResponse sendOtpMobile(UpdateOtpDto otpDto) {
		try {
			if (Objects.isNull(otpDto)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.mobile.number.cant.be.empty");
			}

			if (!StringUtils.hasText(otpDto.getMobileNumber())) {
				return exceptionHandlerUtil.createErrorResponse("api.error.mobile.number.cant.be.empty");
			} else {
				Object obj = null;
				OTPResponseDTO otpResponse = new OTPResponseDTO();
				ApiResponse apiResponse = null;
				String mobileOTP = generateOtp(6);
				System.out.println("mobileOTP >> " + mobileOTP + " : " + AppUtil.encrypt(mobileOTP));

				if (otpDto.getMobileNumber().equals(testIosOtp) || otpDto.getMobileNumber().equals(testAndroidOtp)) {
					ApiResponse apiResponseDemo = verifyOtp(otpDto.getMobileNumber(), null);
					if (apiResponseDemo.isSuccess()) {
						return exceptionHandlerUtil.createSuccessResponse("api.response.ok",
								apiResponseDemo.getResult());
					}
				}

				logger.info(CLASS + "sendOTPMobileSms req IND {}", otpDto.getMobileNumber());
				if (otpDto.getMobileNumber().startsWith("+91")) {
					if (otpDto.getMobileNumber().length() == 13) {
						System.out.println("IND");

						apiResponse = sendSMSIND(mobileOTP, otpDto.getMobileNumber().substring(3, 13));
						if (apiResponse.isSuccess()) {
							otpResponse.setMobileOTP(null);
							otpResponse.setEmailOTP(null);
							otpResponse.setTtl(timeToLive);
							otpResponse.setMobileEncrptyOTP(encryptedString(mobileOTP));

							return exceptionHandlerUtil.createSuccessResponse("api.response.ok", otpResponse);

						} else {
							return AppUtil.createApiResponse(false,
									"Unable to perform action. Please try after sometime", null);
						}
					} else {

						return exceptionHandlerUtil.createErrorResponse(
								"api.error.phone.number.is.invalid.please.enter.correct.phone.number");
					}
				} else if (otpDto.getMobileNumber().startsWith("+256")) {
					if (otpDto.getMobileNumber().length() == 13) {
						logger.info(CLASS + "sendOTPMobileSms req UGA {}", otpDto.getMobileNumber());
						ApiResponse response = sendSMSUGA(mobileOTP, otpDto.getMobileNumber(), timeToLive);
						try {
							SmsOtpResponseDTO smsOtpResponse = objectMapper.readValue(response.getResult().toString(),
									SmsOtpResponseDTO.class);
							if (smsOtpResponse.getNon_field_errors() != null) {

								return exceptionHandlerUtil.createFailedResponseWithCustomMessage(
										smsOtpResponse.getNon_field_errors().get(0), null);
							} else {
								otpResponse.setMobileOTP(null);
								otpResponse.setEmailOTP(null);
								otpResponse.setTtl(timeToLive);
								otpResponse.setMobileEncrptyOTP(encryptedString(mobileOTP));

								return exceptionHandlerUtil.createSuccessResponse("api.response.ok", otpResponse);
							}
						} catch (Exception e) {
							sentryClientExceptions.captureExceptions(e);
							logger.error(CLASS + "sendSMSUGA IN UGA Exception {}", e.getMessage());
							e.printStackTrace();

							return exceptionHandlerUtil
									.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");
						}
					} else {

						return exceptionHandlerUtil.createErrorResponseWithResult(
								"api.error.phone.number.is.invalid.please.enter.correct.phone.number", otpResponse);
					}
				} else if (otpDto.getMobileNumber().startsWith("+971")) {
					if (otpDto.getMobileNumber().length() == 13) {
						logger.info(CLASS + "sendOTPMobileSms req +971 {}", otpDto.getMobileNumber());
						obj = sendSMSUAE(mobileOTP, otpDto.getMobileNumber(), timeToLive);

						try {
							String sms = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
							LinkedHashMap<String, String> smsOtpResponse = objectMapper.readValue(sms,
									LinkedHashMap.class);
							if (smsOtpResponse.get("code") == "406") {
								return exceptionHandlerUtil.createErrorResponse("api.error.invalid.number");
							} else {
								otpResponse.setMobileOTP(null);
								otpResponse.setEmailOTP(null);
								otpResponse.setTtl(timeToLive);
								otpResponse.setMobileEncrptyOTP(encryptedString(mobileOTP));
								return exceptionHandlerUtil.createSuccessResponse("api.response.ok", otpResponse);

							}
						} catch (Exception e) {
							sentryClientExceptions.captureExceptions(e);
							e.printStackTrace();
							logger.error(CLASS + "sendSMSUAE IN UAE Exception {}", e.getMessage());

							return exceptionHandlerUtil
									.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");
						}
					} else {

						return exceptionHandlerUtil.createErrorResponse(
								"api.error.phone.number.is.invalid.please.enter.correct.phone.number");
					}
				}
				return exceptionHandlerUtil.createErrorResponse("api.error.invalid.country.code");
			}

		} catch (Exception e) {
			e.printStackTrace();
			sentryClientExceptions.captureExceptions(e);
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	private ApiResponse sendSMSINDNEW(String otp, String mobileNumber) {
		logger.info(CLASS + "sendSMSIND req  otp {} and mobileNumber {}", otp, mobileNumber);
		String smsBody = "Dear Subscriber, " + otp + " is your DigitalTrust Mobile verification one-time code";
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		String smsUrlWithBody = indApiSMS + "?senderid=DGTRST&channel=Trans&DCS=0&flashsms=0&number=" + mobileNumber
				+ "&text=" + smsBody + "&route=47&PEID=1301162592212041556&user=devesh.mishra@digitaltrusttech.com"
				+ "&password=DigitalTrust@20&DLTTemplateId=1307162619898313468";

		HttpEntity<Object> requestEntity = new HttpEntity<>(headers);
		try {

			logger.info(CLASS + "sendSMSIND req for restTemplate smsUrlWithBody {} and requestEntity {}",
					smsUrlWithBody, requestEntity);

			ResponseEntity<Object> res = restTemplate.exchange(smsUrlWithBody, HttpMethod.GET, requestEntity,
					Object.class);
			String smsResponse = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(res.getBody());
			LinkedHashMap<String, String> indiaSmsOtpResponse = objectMapper.readValue(smsResponse,
					LinkedHashMap.class);
			if (indiaSmsOtpResponse.get("ErrorCode") == "000" || indiaSmsOtpResponse.get("ErrorCode").equals("000")) {
				logger.info(CLASS + "sendSMSIND res for restTemplate {}", indiaSmsOtpResponse);
				return AppUtil.createApiResponse(true, indiaSmsOtpResponse.get("ErrorMessage"), null);
			} else {
				return AppUtil.createApiResponse(false, indiaSmsOtpResponse.get("ErrorMessage"), null);
			}
		} catch (JDBCConnectionException | ConstraintViolationException | DataException | LockAcquisitionException
				| PessimisticLockException | QueryTimeoutException | SQLGrammarException | GenericJDBCException e) {
			e.printStackTrace();
			return AppUtil.createApiResponse(false, "api.error.something.went.wrong.please.try.after.sometime", null);
		} catch (Exception e) {
			logger.error(CLASS + "sendSMSIND Exception {}", e.getMessage());
			e.printStackTrace();
			return AppUtil.createApiResponse(false, "api.error.something.went.wrong.please.try.after.sometime", null);
		}
	}

	private ApiResponse sendSMSIND(String otp, String mobileNumber) {
		logger.info(CLASS + "sendSMSIND req  otp {} and mobileNumber {}", otp, mobileNumber);
		String smsBody = "Dear Subscriber, " + otp + " is your DigitalTrust Mobile verification one-time code";
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		String smsUrlWithBody = indApiSMS
				+ "?APIKey=E2X4Ixz65kKlawWUBVUKkA&senderid=DGTRST&channel=2&DCS=0&flashsms=0&number=" + mobileNumber
				+ "&text=" + smsBody + "&route=1&dlttemplateid=1307162619898313468";

		HttpEntity<Object> requestEntity = new HttpEntity<>(headers);
		try {

			logger.info(CLASS + "sendSMSIND req for restTemplate smsUrlWithBody {} and requestEntity {}",
					smsUrlWithBody, requestEntity);

			ResponseEntity<Object> res = restTemplate.exchange(smsUrlWithBody, HttpMethod.GET, requestEntity,
					Object.class);
			String smsResponse = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(res.getBody());
			LinkedHashMap<String, String> indiaSmsOtpResponse = objectMapper.readValue(smsResponse,
					LinkedHashMap.class);
			if (indiaSmsOtpResponse.get("ErrorCode") == "000" || indiaSmsOtpResponse.get("ErrorCode").equals("000")) {
				logger.info(CLASS + "sendSMSIND res for restTemplate {}", indiaSmsOtpResponse);
				return exceptionHandlerUtil
						.createSuccessResponseWithCustomMessage(indiaSmsOtpResponse.get("ErrorMessage"), null);
			} else {
				return exceptionHandlerUtil
						.createFailedResponseWithCustomMessage(indiaSmsOtpResponse.get("ErrorMessage"), null);
			}
		} catch (Exception e) {
			logger.error(CLASS + "sendSMSIND Exception {}", e.getMessage());
			e.printStackTrace();
			sentryClientExceptions.captureExceptions(e);
			return exceptionHandlerUtil.handleHttpException(e);
		}
	}

	public ApiResponse sendSMSUGA(String otp, String mobileNumber, int timeToLive) throws ParseException {
		logger.info(CLASS + "sendSMSUGA otp {} and mobileNumber {} and timeToLive {} ", otp, mobileNumber, timeToLive);
		String url = niraApiSMS;
		String basicAuth = getBasicAuth();
		SmsDTO smsDTO = new SmsDTO();
		smsDTO.setPhoneNumber(mobileNumber);
		smsDTO.setSmsText("Dear Customer, your OTP for UgPass Registration is " + otp
				+ ", Please use this OTP to validate your Mobile number. This OTP is valid for " + timeToLive
				+ " Seconds - UgPass System");
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("daes-authorization", basicAuth);
		headers.set("access_token", getToken());
		HttpEntity<Object> requestEntity = new HttpEntity<>(smsDTO, headers);
		try {
			logger.info(CLASS + " sendSMSUGA req for restTemplate url {} and requestEntity {} ", url, requestEntity);
			ResponseEntity<ApiResponse> res = restTemplate.exchange(url, HttpMethod.POST, requestEntity,
					ApiResponse.class);
			ApiResponse api = res.getBody();
			logger.info("sendSMSUGA res for restTemplate {}", res);
			return api;
		} catch (Exception e) {
			logger.error(CLASS + "sendSMSUGA Exception {}", e.getMessage());
			e.printStackTrace();
			return exceptionHandlerUtil.handleHttpException(e);
		}
	}

	public Object sendSMSUAE(String otp, String mobileNumber, int timeToLive) throws ParseException {
		logger.info("sendSMSUAE  otp {} and mobileNumber {} and timeToLive {}", otp, mobileNumber, timeToLive);
		String url = uaeApiSMS;
		String text = "Your ICA-Pass OTP Phone verification code  is " + otp + "The code is valid for " + timeToLive
				+ " seconds. Don't share this code with anyone.";

		Map<String, String> uaeSmsBody = new HashMap<String, String>();
		uaeSmsBody.put("mobileno", mobileNumber);
		uaeSmsBody.put("smstext", text);

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		System.out.println("getToken() :: " + getToken());
		headers.set("access_token", getToken());
		HttpEntity<Object> requestEntity = new HttpEntity<>(uaeSmsBody, headers);
		try {
			logger.info(CLASS + "sendSMSUAE req for restTemplate url {} and requestEntity {} ", url, requestEntity);
			ResponseEntity<Object> res = restTemplate.exchange(url, HttpMethod.POST, requestEntity, Object.class);
			ApiResponse api = new ApiResponse();
			api.setSuccess(true);
			api.setMessage("");
			api.setResult(res.getBody());
			logger.info(CLASS + "sendSMSUAE res for restTemplate {}", res);
			return api.getResult();
		} catch (Exception e) {
			logger.error("sendSMSUAE Exception {}", e.getMessage());
			e.printStackTrace();
			return exceptionHandlerUtil.handleHttpException(e);

//			return AppUtil.createApiResponse(false,
//					"api.error.something.went.wrong.please.try.after.sometime", null);
		}
	}

	public String getBasicAuth() {
		String userCredentials = niraUserName + ":" + niraPassword;
		String basicAuth = new String(Base64.getEncoder().encode(userCredentials.getBytes()));
		return basicAuth;
	}

	public String getToken() {
		String url = niraApiToken;
		logger.info(CLASS + "getToken req url {}", url);
		String basicAuth = getBasicAuth();
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("daes-authorization", basicAuth);
		HttpEntity<Object> requestEntity = new HttpEntity<>(headers);
		try {
			logger.info(CLASS + "getToken req for restTemplate {}", requestEntity);
			ResponseEntity<String> res = restTemplate.exchange(url, HttpMethod.GET, requestEntity, String.class);
			logger.info(CLASS + "getToken res for restTemplate {}", res);
			return res.getBody();
		} catch (Exception e) {
			logger.error(CLASS + "getToken Exception {}", e.getMessage());
			e.printStackTrace();
			return e.getMessage();
		}

	}

	public String generatecorrelationIdUniqueId() {
		UUID correlationID = UUID.randomUUID();
		return correlationID.toString();
	}

	public String generateOtp(int maxLength) {
		try {
			SecureRandom secureRandom = SecureRandom.getInstance("SHA1PRNG");
			StringBuilder otp = new StringBuilder(maxLength);

			for (int i = 0; i < maxLength; i++) {
				otp.append(secureRandom.nextInt(9));
			}
			return otp.toString();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public ApiResponse sendEmailToSubscriber(EmailReqDto emailReqDto) {
		try {
			sentryClientExceptions.captureTags(null, emailReqDto.getEmailId(), "sendEmailToSubscriber",
					"SubscriberServiceImpl");
			String url = emailBaseUrl;
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<Object> requestEntity = new HttpEntity<>(emailReqDto, headers);
			System.out.println("requestEntity >> " + requestEntity);
			ResponseEntity<ApiResponse> res = restTemplate.exchange(url, HttpMethod.POST, requestEntity,
					ApiResponse.class);
			System.out.println("res >> " + res);
			if (res.getStatusCodeValue() == 200) {
				System.out.println(" sendEmailToSubscriber res.getBody().getMessage() " + res.getBody().getMessage());
				return exceptionHandlerUtil.createSuccessResponse("api.response.sent", res);
			} else if (res.getStatusCodeValue() == 400) {

				return exceptionHandlerUtil.createSuccessResponse("api.error.bad.request", res);

			} else if (res.getStatusCodeValue() == 500) {

				return exceptionHandlerUtil.createSuccessResponse("api.error.internal.server.error", res);

			}
			return exceptionHandlerUtil.createFailedResponseWithCustomMessage(res.getBody().getMessage(), null);
		} catch (Exception e) {
			e.printStackTrace();
			sentryClientExceptions.captureExceptions(e);
			return exceptionHandlerUtil.handleHttpException(e);

		}

	}

	@Override
	public ApiResponse reOnboardAddSubscriberObData(SubscriberObRequestDTO obRequestDTO) throws Exception {
		try {
			if (Objects.isNull(obRequestDTO)) {
				return exceptionHandlerUtil
						.createErrorResponse("api.error.subscriber.ob.request.cant.be.null.or.empty");
			}
			String validationMessage = ValidationUtil.validate(obRequestDTO);
			if (validationMessage != null) {
				System.out.println(" reOnboardAddSubscriberObData Validation errors: " + validationMessage);
				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(validationMessage, null);
			}

			String subscriberOBDataValidation = ValidationUtil.validate(obRequestDTO.getSubscriberData());
			if (subscriberOBDataValidation != null) {
				System.out.println(" reOnboardAddSubscriberObData Validation errors: " + subscriberOBDataValidation);
				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(subscriberOBDataValidation, null);
			}

			if (obRequestDTO.getSubscriberData().getIssuingState() == null
					|| obRequestDTO.getSubscriberData().getIssuingState().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.issuing.state.cant.be.null.or.empty");
			}

			SubscriberOnboardingData subscriberOnboardingData = onboardingDataRepoIface
					.findLatestSubscriber(subscriber.getSubscriberUid()).stream().findFirst().orElse(null);

			String oldExpireDate = subscriberOnboardingData.getDateOfExpiry();

			long differenceInDays = AppUtil.getDifferenceBetDates(subscriberOnboardingData.getCreatedDate());
			logger.info(CLASS + "reOnboardAddSubscriberObData request {}", obRequestDTO);
			Subscriber subscriberData = subscriberRepoIface.findBysubscriberUid(obRequestDTO.getSuID());
			if (subscriberData == null) {
				return AppUtil.createApiResponse(false, "api.error.subscriber.not.found", null);
			}
			SubscriberObData subscriberObData = obRequestDTO.getSubscriberData();
			// check gender must be the same
			if (checkGender) {
				String gender1 = normalizeGender(subscriberObData.getGender());
				String gender2 = normalizeGender(subscriberOnboardingData.getGender());
				if (!gender1.equals(gender2)) {
					return exceptionHandlerUtil.createErrorResponse("api.error.gender.must.be.same");
				}
			}

			String dob = AppUtil.removeTimeStamp(subscriberData.getDateOfBirth());
			String reOnboardDOB = AppUtil.removeTimeStamp(subscriberObData.getDateOfBirth());

			if (checkDateOfBirth) {
				if (!reOnboardDOB.equals(dob)) {
					return exceptionHandlerUtil.createErrorResponse("api.error.date.of.birth.must.be.same");
				}
			}

			// check id document number must be different
			if (subscriberObData.getDocumentNumber() == null) {
				return exceptionHandlerUtil.createErrorResponse("api.error.id.document.number.cant.be.null");
			}
			String latest = AppUtil.getDate().toString();
			// check expiry date of old id. if expired allow to update
			Subscriber subscriber2 = subscriberRepoIface.findbyDocumentNumber(subscriberObData.getDocumentNumber());
			if (subscriber2 != null) {
				if (!subscriber2.getSubscriberUid().equals(obRequestDTO.getSuID())) {
					return exceptionHandlerUtil.createErrorResponse("api.error.this.document.is.already.onboarded");
				}
			}

			if (oldExpireDate.compareTo(latest) < 0) {
				// new document date of expiry
				LocalDateTime newExpiryDate = AppUtil.getLocalDateTime(subscriberObData.getDateOfExpiry().toString());
				// new current date and time
				LocalDateTime currentDateTime = AppUtil.getLocalDateTime(AppUtil.getDate().toString());

				if (!"NIN".equalsIgnoreCase(obRequestDTO.getOnboardingMethod())) {
					if (newExpiryDate.isAfter(currentDateTime)) {
						long daysBetween = Duration.between(currentDateTime, newExpiryDate).toDays();
						System.out.println("Days: " + daysBetween);
						if (daysBetween <= 1) {
							return exceptionHandlerUtil.createErrorResponse(
									"api.error.you.cant.do.reonboard.because.your.document.date.of.expiry.is.less.then.days");
						}
					} else if (newExpiryDate.isBefore(currentDateTime)) {
						return exceptionHandlerUtil.createErrorResponse(
								"api.error.the.expiry.date.with.time.is.before.the.current.date.with.time");
					} else {
						return exceptionHandlerUtil.createErrorResponse(
								"api.error.the.expiry.date.with.time.is.the.same.as.the.current.date.with.time");
					}
				}

//                if (newExpiryDate.isAfter(currentDateTime)) {
//                    long daysBetween = Duration.between(currentDateTime, newExpiryDate).toDays();
//                    if (daysBetween <= 1) {
//                        return exceptionHandlerUtil.createErrorResponse(
//                                "api.error.you.cant.do.reonboard.because.your.document.date.of.expiry.is.less.then.days");
//                    }
//                } else if (newExpiryDate.isBefore(currentDateTime)) {
//                    return exceptionHandlerUtil.createErrorResponse(
//                            "api.error.the.expiry.date.with.time.is.before.the.current.date.with.time");
//                } else {
//                    return exceptionHandlerUtil.createErrorResponse(
//                            "api.error.the.expiry.date.with.time.is.the.same.as.the.current.date.with.time");
//                }

				// check LOA level
				String loa = subscriberOnboardingData.getLevelOfAssurance();

				if (loa.equals(Constant.LOA1)) {
					// if(obRequestDTO.getOnboardingMethod().equals("UNID") ||
					// obRequestDTO.getOnboardingMethod().equals("UNID"))
				} else if (loa.equals(Constant.LOA2)) {
					if (obRequestDTO.getOnboardingMethod().equals(Constant.UNID)) {
						return exceptionHandlerUtil
								.createErrorResponse("api.error.you.are.using.low.level.of.assurance");
					}
				} else if (loa.equals(Constant.LOA3)) {
					if (obRequestDTO.getOnboardingMethod().equals(Constant.UNID)) {
						return exceptionHandlerUtil
								.createErrorResponse("api.error.you.are.using.low.level.of.assurance");
					}
					if (obRequestDTO.getOnboardingMethod().equals(Constant.PASSPORT)) {
						return exceptionHandlerUtil
								.createErrorResponse("api.error.you.are.using.low.level.of.assurance");
					}
				}
				return addSubscriberObData(obRequestDTO);

			} else {
				if (differenceInDays < expiryDays && !obRequestDTO.getSubscriberData().getDocumentNumber()
						.equals(subscriberOnboardingData.getIdDocNumber())) {
					return exceptionHandlerUtil.createFailedResponseWithCustomMessage(
							"We can't processed. it's seem your last updation of your id document is less than "
									+ expiryDays + " days.",
							null);
				} else {
					if (checkDocumentNumber) {
						SubscriberView subscriberCertRevoked = subscriberViewRepoIface
								.findSubscriberByDocIdCertRevoked(subscriberObData.getDocumentNumber());
						if (subscriberCertRevoked != null) {
							if (subscriberObData.getDocumentNumber().equals(subscriberCertRevoked.getIdDocNumber())) {
								return exceptionHandlerUtil
										.createErrorResponse("api.error.id.document.number.must.be.different");
							}
						}
					}

					// new document date of expiry
					LocalDateTime newExpiryDate = AppUtil
							.getLocalDateTime(subscriberObData.getDateOfExpiry().toString());
					// new current date and time
					LocalDateTime currentDateTime = AppUtil.getLocalDateTime(AppUtil.getDate().toString());
//                    if (newExpiryDate.isAfter(currentDateTime)) {
//                        long daysBetween = Duration.between(currentDateTime, newExpiryDate).toDays();
//                        System.out.println("Days: " + daysBetween);
//                        if (daysBetween <= 1) {
//                            return exceptionHandlerUtil.createErrorResponse(
//                                    "api.error.you.cant.do.reonboard.because.your.document.date.of.expiry.is.less.then.days");
//                        }
//                    } else if (newExpiryDate.isBefore(currentDateTime)) {
//                        return exceptionHandlerUtil.createErrorResponse(
//                                "api.error.the.expiry.date.with.time.is.before.the.current.date.with.time");
//                    } else {
//                        return exceptionHandlerUtil.createErrorResponse(
//                                "api.error.the.expiry.date.with.time.is.the.same.as.the.current.date.with.time");
//                    }

					if (!"NIN".equalsIgnoreCase(obRequestDTO.getOnboardingMethod())) {
						if (newExpiryDate.isAfter(currentDateTime)) {
							long daysBetween = Duration.between(currentDateTime, newExpiryDate).toDays();
							System.out.println("Days: " + daysBetween);
							if (daysBetween <= 1) {
								return exceptionHandlerUtil.createErrorResponse(
										"api.error.you.cant.do.reonboard.because.your.document.date.of.expiry.is.less.then.days");
							}
						} else if (newExpiryDate.isBefore(currentDateTime)) {
							return exceptionHandlerUtil.createErrorResponse(
									"api.error.the.expiry.date.with.time.is.before.the.current.date.with.time");
						} else {
							return exceptionHandlerUtil.createErrorResponse(
									"api.error.the.expiry.date.with.time.is.the.same.as.the.current.date.with.time");
						}
					}

					// check LOA level
					String loa = subscriberOnboardingData.getLevelOfAssurance();

					if (loa.equals(Constant.LOA1)) {
						// if(obRequestDTO.getOnboardingMethod().equals("UNID") ||
						// obRequestDTO.getOnboardingMethod().equals("UNID"))
					} else if (loa.equals(Constant.LOA2)) {
						if (obRequestDTO.getOnboardingMethod().equals(Constant.UNID)) {

							return exceptionHandlerUtil
									.createErrorResponse("api.error.you.are.using.low.level.of.assurance");
						}
					} else if (loa.equals(Constant.LOA3)) {
						if (obRequestDTO.getOnboardingMethod().equals(Constant.UNID)) {

							return exceptionHandlerUtil
									.createErrorResponse("api.error.you.are.using.low.level.of.assurance");
						}
						if (obRequestDTO.getOnboardingMethod().equals(Constant.PASSPORT)) {
							return exceptionHandlerUtil
									.createErrorResponse("api.error.you.are.using.low.level.of.assurance");
						}
					}
					return addSubscriberObData(obRequestDTO);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			sentryClientExceptions.captureTags(obRequestDTO.getSuID(), null, "reOnboardAddSubscriberObData",
					"SubscriberController");
			sentryClientExceptions.captureExceptions(e);
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	private String normalizeGender(String gender) {
		if (gender == null)
			return "";
		gender = gender.trim().toLowerCase();
		if (gender.equals("m") || gender.equals("male")) {
			return "male";
		} else if (gender.equals("f") || gender.equals("female")) {
			return "female";
		}
		return gender; // fallback if value is unexpected
	}

	@Override
	public ApiResponse deleteRecord(String mobileNo, String email) {
		try {
			if (!mobileNo.equals("")) {
				Optional<Subscriber> subscriber = Optional
						.ofNullable(subscriberRepoIface.findBymobileNumber("+" + mobileNo));// subscriberRepoIface.findByemailId(email);
				if (subscriber.isPresent()) {
					String suid = subscriber.get().getSubscriberUid();
					// int a = subscriberRepoIface.deleteRecordBySubscriberUid(suid);
					subscriberDeletionRepository.deleteSubscriberRecord(suid);
					int a = 1;
					if (a == 1) {
						return exceptionHandlerUtil
								.successResponse("api.response.subscriber.record.deleted.successfully");
					} else {
						return exceptionHandlerUtil
								.createErrorResponse("api.error.subscriber.record.not.deleted.successfully");
					}
				}
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.not.found");
			} else {
				Optional<Subscriber> subscriber = Optional.ofNullable(subscriberRepoIface.findByemailId(email));// subscriberRepoIface.findByemailId(email);
				if (subscriber.isPresent()) {
					String suid = subscriber.get().getSubscriberUid();
					subscriberDeletionRepository.deleteSubscriberRecord(suid);
					int a = 1;
					if (a == 1) {
						return exceptionHandlerUtil
								.successResponse("api.response.subscriber.record.deleted.successfully");
					} else {
						return exceptionHandlerUtil
								.createErrorResponse("api.error.subscriber.record.not.deleted.successfully");
					}
				}
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.not.found");
			}
		} catch (Exception e) {
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	@Override
	public ApiResponse getDeviceStatus(HttpServletRequest httpServletRequest) {
		try {
			String deviceId = httpServletRequest.getHeader("deviceId");
			if (deviceId == null || deviceId.equals("")) {
				return exceptionHandlerUtil.createErrorResponse("api.error.deviceid.not.coming.please.send.deviceid");
			}

			// SubscriberDevice subscriberDevice =
			// deviceRepoIface.findBydeviceUid(deviceId);

//			Optional<SubscriberDevice> subscriberDevice = Optional
//					.ofNullable((SubscriberDevice) deviceRepoIface.findBydeviceUid(deviceId));
			List<SubscriberDevice> subscriberDeviceList = deviceRepoIface.findBydeviceUid(deviceId);
			// Most recent one, since you sorted DESC

			SubscriberDevice subscriberDevices = getLatest(subscriberDeviceList);

			System.out.println("latestttttttttttt::::::::::::::" + subscriberDevices);
			DeviceStatusDto deviceStatusDto = new DeviceStatusDto();
			if (subscriberDevices != null) {
				SubscriberFcmToken subscriberFcmToken = fcmTokenRepoIface
						.findBysubscriberUid(subscriberDevices.getSubscriberUid());
				deviceStatusDto.setFcmToken(subscriberFcmToken.getFcmToken());
				deviceStatusDto.setDeviceStatus(subscriberDevices.getDeviceStatus());
				if (subscriberDevices.getDeviceStatus().equals(Constant.DEVICE_STATUS_DISABLED)) {
					deviceStatusDto.setConsentRequired(false);

					return exceptionHandlerUtil.createSuccessResponse("api.response.device.status.is.disabled",
							deviceStatusDto);
				} else {
					SubscriberStatus subscriberStatus = statusRepoIface
							.findBysubscriberUid(subscriberDevices.getSubscriberUid());
					if (signRequired) {
						if (subscriberStatus.getSubscriberStatus().equals("ACTIVE")) {
							List<ConsentHistory> latestConsentList = consentHistoryRepo.findLatestConsent();
							ConsentHistory consentHistory = latestConsentList.isEmpty() ? null
									: latestConsentList.get(0);

							if (consentHistory == null) {
								deviceStatusDto.setConsentRequired(false);
							} else {
								SubscriberConsents subscriberConsents = subscriberConsentsRepo
										.findSubscriberConsentBySuidAndConsentId(subscriberDevices.getSubscriberUid(),
												consentHistory.getId());
								if (subscriberConsents == null) {
									deviceStatusDto.setConsentRequired(true);
								} else {
									deviceStatusDto.setConsentRequired(false);
								}
							}
						} else {
							deviceStatusDto.setConsentRequired(false);
						}
					} else {
						List<ConsentHistory> latestConsentList = consentHistoryRepo.findLatestConsent();
						ConsentHistory consentHistory = latestConsentList.isEmpty() ? null : latestConsentList.get(0);

						if (consentHistory == null) {
							deviceStatusDto.setConsentRequired(false);
						} else {
							SubscriberConsents subscriberConsents = subscriberConsentsRepo
									.findSubscriberConsentBySuidAndConsentId(subscriberDevices.getSubscriberUid(),
											consentHistory.getId());
							if (subscriberConsents == null) {
								deviceStatusDto.setConsentRequired(true);
							} else {
								deviceStatusDto.setConsentRequired(false);
							}
						}
					}
					return exceptionHandlerUtil.createSuccessResponse("api.response.device.status", deviceStatusDto);
				}
			} else {
//				Optional<SubscriberDeviceHistory> subscriberDeviceHistory = Optional
//						.ofNullable((SubscriberDeviceHistory) subscriberDeviceHistoryRepoIface.findBydeviceUid(deviceId));
//

				List<SubscriberDeviceHistory> historyList = subscriberDeviceHistoryRepoIface.findBydeviceUid(deviceId);
				SubscriberDeviceHistory latest = historyList.isEmpty() ? null : historyList.get(0);
				Optional<SubscriberDeviceHistory> subscriberDeviceHistory = Optional.ofNullable(latest);

				if (subscriberDeviceHistory.isPresent()) {
					deviceStatusDto.setDeviceStatus(subscriberDeviceHistory.get().getDeviceStatus());
					deviceStatusDto.setConsentRequired(false);

					return exceptionHandlerUtil.createSuccessResponse("api.response.device.status.is.disabled",
							deviceStatusDto);
				} else {
					deviceStatusDto.setConsentRequired(false);
					deviceStatusDto.setDeviceStatus(Constant.NEW_DEVICE);
					return exceptionHandlerUtil.createSuccessResponse("api.response.device.status", deviceStatusDto);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}

	}

	public SubscriberDevice getLatest(List<SubscriberDevice> list) {

		// List<SubscriberDevice> list = deviceRepoIface.findByDeviceUid(deviceUid);

		return list.stream()
				.sorted(Comparator.comparing(sd -> parseDate(sd.getUpdatedDate()), Comparator.reverseOrder()))
				.findFirst().orElse(null);
	}

	private LocalDateTime parseDate(String date) {
		if (date.contains("T")) {
			return LocalDateTime.parse(date); // ISO format
		} else {
			return LocalDateTime.parse(date.replace(" ", "T"));
		}
	}

	@Override
	public ApiResponse getSubscriberDetailsBySerachType(String searchType, String searchValue) {
		try {
			logger.info(CLASS + "getSubscriberDetailsBySerachType request searchType and searchValue {},{}", searchType,
					searchValue);

			if (searchType == null || searchType.isEmpty() || searchValue == null || searchValue.isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.bad.request");
			}

			Subscriber subscriber = null;
			SubscriberDeviceUpdateDto subscriberDeviceUpdateDto = new SubscriberDeviceUpdateDto();
			switch (searchType) {
			case "emailId":
				subscriber = subscriberRepoIface.findByemailId(searchValue);
				break;
			case "mobileNumber":
				subscriber = subscriberRepoIface.findBymobileNumber(searchValue);
				break;
			default:
				return exceptionHandlerUtil.createErrorResponse("api.error.bad.request");
			}
			if (subscriber == null) {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.details.not.found");
			} else {

				SubscriberStatus subscriberStatus = statusRepoIface.findBysubscriberUid(subscriber.getSubscriberUid());

				SubscriberDevice subscriberDevice = deviceRepoIface.getSubscriber(subscriber.getSubscriberUid());
				subscriberDeviceUpdateDto.setSubscriberUid(subscriber.getSubscriberUid());
				subscriberDeviceUpdateDto.setFullName(subscriber.getFullName());
				subscriberDeviceUpdateDto.setDateOfBirth(subscriber.getDateOfBirth());
				subscriberDeviceUpdateDto.setIdDocType(subscriber.getIdDocType());
				subscriberDeviceUpdateDto.setIdDocNumber(subscriber.getIdDocNumber());
				subscriberDeviceUpdateDto.seteMail(subscriber.getEmailId());
				subscriberDeviceUpdateDto.setMobileNumber(subscriber.getMobileNumber());
				subscriberDeviceUpdateDto.setOsName(subscriber.getOsName());
				subscriberDeviceUpdateDto.setAppVersion(subscriber.getAppVersion());
				subscriberDeviceUpdateDto.setOsVersion(subscriber.getOsVersion());
				subscriberDeviceUpdateDto.setDeviceInfo(subscriber.getDeviceInfo());

				subscriberDeviceUpdateDto.setCreatedDate(subscriber.getCreatedDate());
				subscriberDeviceUpdateDto.setUpdatedDate(subscriber.getUpdatedDate());

				subscriberDeviceUpdateDto.setSubscriberStatus(subscriberStatus.getSubscriberStatus());

				subscriberDeviceUpdateDto.setDeviceUid(subscriberDevice.getDeviceUid());
				subscriberDeviceUpdateDto.setDeviceStatus(subscriberDevice.getDeviceStatus());

				// SusbcriberDetailsView subscriberDetails =
				// susbcriberDetailsRepository.findBysubscriberUid(subscriber.getSubscriberUid());

				return exceptionHandlerUtil.createSuccessResponse("api.response.subscriber.details",
						subscriberDeviceUpdateDto);
			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + " getSubscriberDetailsBySerachType Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Override
	public ApiResponse updateSusbcriberDeviceStatus(String suid) {
		try {
			logger.info(CLASS + "updateSusbcriberDeviceStatus request suid {}", suid);
			if (suid == null || suid.trim() == "") {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.unique.id.cant.be.null");
			} else {
				SubscriberDevice subscriberDevice = (SubscriberDevice) deviceRepoIface.getSubscriberDeviceStatus(suid);
				if (subscriberDevice == null) {
					return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.details.not.found");
				} else {
					subscriberDevice.setDeviceStatus(Constant.DEVICE_STATUS_DISABLED);
					subscriberDevice.setUpdatedDate(AppUtil.getDate());
					deviceRepoIface.save(subscriberDevice);
					return exceptionHandlerUtil.successResponse("api.response.subscriber.device.status.updated");
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + " updateSusbcriberDeviceStatus Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Override
	public ApiResponse getSubscriberListBySerachType(String searchType, String searchValue) {
		try {
			logger.info(CLASS + "getSubscriberDetailsBySerachType request searchType and searchValue {},{}", searchType,
					searchValue);
			if (searchType == null || searchType.isEmpty() || searchValue == null || searchValue.isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.bad.request");
			}

			List<String> subscriberList = null;
			switch (searchType) {
			case "emailId":
				subscriberList = subscriberRepoIface.getSubscriberListByEmailId(searchValue);
				break;
			case "mobileNumber":
				subscriberList = subscriberRepoIface.getSubscriberListByMobileNo(searchValue);
				break;
			default:
				return exceptionHandlerUtil.createErrorResponse("api.error.bad.request");
			}
			if (subscriberList == null) {

				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.details.not.found");
			} else {

				String jsonToString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(subscriberList);
				return AppUtil.createApiResponse(true, "api.response.subscriber.details", jsonToString);
			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + " getSubscriberDetailsBySerachType Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Override
	public ApiResponse updateFcmTokenDetails(String suid, String fcmToken) {
		try {
			logger.info("{}{} - Received request to update FCM token for suid: {} with fcmToken: {}", CLASS,
					Utility.getMethodName(), suid, fcmToken);
			Date startTime = new Date();
			if (suid != null && suid != "") {
				if (fcmToken != null && fcmToken != "") {
					SubscriberFcmToken subscriberFcmToken = fcmTokenRepoIface.findBysubscriberUid(suid);
					if (subscriberFcmToken != null) {
						String message = "OLD FCMTOKEN | " + subscriberFcmToken.getFcmToken() + " NEW FCMTOKEN |"
								+ fcmToken;
						logger.info("{}{} - message {}", CLASS, Utility.getMethodName(), message);
						subscriberFcmToken.setFcmToken(fcmToken);
						subscriberFcmToken.setCreatedDate(AppUtil.getDate());
						fcmTokenRepoIface.save(subscriberFcmToken);
						Date endTime = new Date();
						logModelServiceImpl.setLogModelFCMToken(true, suid, null, "OTHER", null, message, startTime,
								endTime, null);
						return exceptionHandlerUtil.createSuccessResponse("api.response.fcmtoken.updated.successfully",
								subscriberFcmToken);
					} else {
						return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.not.found");
					}
				} else {
					return exceptionHandlerUtil.createErrorResponse("api.error.fcmtoken.cant.be.null.or.empty");
				}
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.suid.cantbe.null.or.empty");
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + " updateFcmTokenDetails Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Override
	public ApiResponse getSubDetailsBySerachType(HttpServletRequest httpServletRequest, String searchType,
			String searchValue) {
		try {
			logger.info(CLASS + "getSubscriberDetailsBySerachType request searchType and searchValue {},{}", searchType,
					searchValue);
			if (searchType == null || searchType.isEmpty() || searchValue == null || searchValue.isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.bad.request");
			}
			MobileOTPDto mobileOTPDto = new MobileOTPDto();
			DeviceInfo deviceInfo = new DeviceInfo();
			Subscriber subscriber = null;
			switch (searchType) {
			case "emailId":
				subscriber = subscriberRepoIface.findByemailId(searchValue);
				break;
			case "mobileNumber":
				subscriber = subscriberRepoIface.findBymobileNumber(searchValue);
				break;
			case "idDocNumber":
				subscriber = subscriberRepoIface.findByIdDocNumber(searchValue);
				break;
			case "nationalId":
				subscriber = subscriberRepoIface.findByNationalId(searchValue);
				break;
			default:
				return exceptionHandlerUtil.createErrorResponse("api.error.bad.request");
			}

			if (subscriber == null) {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.details.not.found");
			} else {
				// For android
				if (subscriber.getMobileNumber().equalsIgnoreCase(testAndroidOtp)
						&& subscriber.getEmailId().equalsIgnoreCase(testAndroidEmail)) {
					SubscriberDevice subscriberDevice = (SubscriberDevice) deviceRepoIface
							.findBysubscriberUid(subscriber.getSubscriberUid());
					subscriberDevice.setDeviceUid(httpServletRequest.getHeader("deviceId"));
					deviceRepoIface.save(subscriberDevice);
				}
				// same for IOS
				if (subscriber.getMobileNumber().equalsIgnoreCase(testIosOtp)
						&& subscriber.getEmailId().equalsIgnoreCase(testIosEmail)) {
					SubscriberDevice subscriberDevice = (SubscriberDevice) deviceRepoIface
							.findBysubscriberUid(subscriber.getSubscriberUid());

					subscriberDevice.setDeviceUid(httpServletRequest.getHeader("deviceId"));
					deviceRepoIface.save(subscriberDevice);
				}

				deviceInfo.setDeviceId(httpServletRequest.getHeader("deviceId"));
				deviceInfo.setAppVersion(httpServletRequest.getHeader("appVersion"));
				deviceInfo.setOsVersion(httpServletRequest.getHeader("osVersion"));

				mobileOTPDto.setSubscriberEmail(subscriber.getEmailId());
				mobileOTPDto.setSubscriberMobileNumber(subscriber.getMobileNumber());
				ApiResponse apiResponse = deviceUpdateIface.validateSubscriberAndDevice(deviceInfo, mobileOTPDto);
				if (apiResponse.isSuccess()) {
					return exceptionHandlerUtil.createSuccessResponse("api.response.subscriber.details",
							apiResponse.getResult());
				} else {
					return exceptionHandlerUtil.createFailedResponseWithCustomMessage(apiResponse.getMessage(), null);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + " getSubDetailsBySerachType Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Override
	public ApiResponse getSusbcriberDeviceHistory(String suid) {
		try {
			logger.info("{}{} - Reuest for SusbcriberDeviceHistory suid {}", CLASS, Utility.getMethodName(), suid);
			if (!StringUtils.hasText(suid)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.id.can.be.null.or.empty");
			}

			Subscriber subscriber = subscriberRepoIface.findBysubscriberUid(suid);
			if (subscriber == null) {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.details.not.found");
			} else {
				SubscriberDeviceHistoryDetails subscriberDeviceHistoryDetails = new SubscriberDeviceHistoryDetails();

				// SubscriberDevice subscriberDevice = (SubscriberDevice)
				// deviceRepoIface.findBysubscriberUid(suid);

				List<SubscriberDevice> devices = deviceRepoIface.findBysubscriberUid(suid);
				SubscriberDevice subscriberDevice = null;
				if (!devices.isEmpty()) {
					subscriberDevice = devices.get(0); // or handle multiple results
				}

				List<SubscriberDeviceHistory> subscriberDeviceHistory = subscriberDeviceHistoryRepoIface
						.findSubscriberDeviceHistory(suid);

				List<HashMap<String, String>> listOfMaps = subscriberDeviceHistory.stream().map(s -> {
					HashMap<String, String> strMap = new HashMap<>();
					strMap.put("device_uid", s.getDeviceUid());
					strMap.put("created_date", s.getCreatedDate());
					return strMap;
				}).collect(Collectors.toList());

				subscriberDeviceHistoryDetails.setSubscriber(subscriber);
				subscriberDeviceHistoryDetails.setSubscriberDevice(subscriberDevice);
				subscriberDeviceHistoryDetails.setSubscriberDeviceHistory(listOfMaps);
				return exceptionHandlerUtil.createSuccessResponse("api.response.subscriber.details",
						subscriberDeviceHistoryDetails);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + " getSusbcriberDeviceHistory Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Override
	public ApiResponse getTotp(TotpDto totpDto) {
		ResponseEntity<ApiResponse> res = null;
		try {
			// System.out.println(" totpDto " + totpDto);
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<TotpDto> requestEntity = new HttpEntity<>(totpDto, headers);
			res = restTemplate.exchange(dtportal, HttpMethod.POST, requestEntity, ApiResponse.class);
			// System.out.println("status code - " + res.getStatusCodeValue());
			if (res.getStatusCodeValue() == 400 || res.getStatusCodeValue() == 401 || res.getStatusCodeValue() == 403
					|| res.getStatusCodeValue() == 404 || res.getStatusCodeValue() == 415
					|| res.getStatusCodeValue() == 500 || res.getStatusCodeValue() == 501
					|| res.getStatusCodeValue() == 503) {
				return exceptionHandlerUtil
						.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");

			} else if (res.getStatusCodeValue() == 200 || res.getStatusCodeValue() == 201) {
				// System.out.println("res- " + res);
				return exceptionHandlerUtil.createSuccessResponseWithCustomMessage("", res.getBody().getResult());
			}
		} catch (Exception e) {
			e.printStackTrace();
			return exceptionHandlerUtil.handleHttpException(e);
		}
		return exceptionHandlerUtil.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");

	}

	@Override
	public ApiResponse getFCMToken(String subscriberUid) {
		try {
			if (!StringUtils.hasText(subscriberUid)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.id.can.be.null.or.empty");
			} else {
				SubscriberFcmToken subscriberFcmToken = fcmTokenRepoIface.findBysubscriberUid(subscriberUid);
				if (subscriberFcmToken != null) {
					return exceptionHandlerUtil.createSuccessResponse(
							"api.response.subscriber.fcm.token.found.successfully", subscriberFcmToken);
				} else {
					return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.fcm.token.not.found");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Override
	public ApiResponse deleteRecordBySuid(String subscriberUid) {
		try {
			Subscriber subscriber = subscriberRepoIface.findBysubscriberUid(subscriberUid);
			if (subscriber != null) {
				// int a = subscriberRepoIface.deleteRecordBySubscriberUid(subscriberUid);
				subscriberDeletionRepository.deleteSubscriberRecord(subscriberUid);
				return exceptionHandlerUtil.successResponse("api.response.subscriber.record.deleted.successfully");
			} else
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.not.found");
		} catch (JDBCConnectionException | ConstraintViolationException | DataException | LockAcquisitionException
				| PessimisticLockException | QueryTimeoutException | SQLGrammarException | GenericJDBCException e) {
			e.printStackTrace();
			return exceptionHandlerUtil.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");
		} catch (Exception e) {
			e.printStackTrace();
			return exceptionHandlerUtil.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");
		}
	}

	@Override
	public ApiResponse getAllSubscribersDataFromView() {
		try {
			System.out.println(" inside getAllSubscribersDataFromView implimentation");
			List<SubscriberCompleteDetail> subscriberCompleteDetailsList = subscriberCompleteDetailRepoIface
					.getAllActiveSubscribersDetails(Constant.ACTIVE);
			if (subscriberCompleteDetailsList == null) {
				return exceptionHandlerUtil.createErrorResponse("api.error.no.data.found");
			}
			ArrayList<SubscriberDetailsDto> SubscriberDetailsDtoList = new ArrayList();
			for (SubscriberCompleteDetail details : subscriberCompleteDetailsList) {
				SubscriberDetailsDto subscriberDetailsDto = new SubscriberDetailsDto();
				subscriberDetailsDto.setEmail(details.getEmailId());
				subscriberDetailsDto.setPhoneNo(details.getMobileNumber());
				subscriberDetailsDto.setFullName(details.getFullName());
				subscriberDetailsDto.setSubscriberStatus(details.getSubscriberStatus());
				SubscriberDetailsDtoList.add(subscriberDetailsDto);
			}
			return exceptionHandlerUtil.successResponse("api.response.subscriber.details");

		} catch (JDBCConnectionException | ConstraintViolationException | DataException | LockAcquisitionException

				| PessimisticLockException | QueryTimeoutException | SQLGrammarException | GenericJDBCException e) {

			e.printStackTrace();
			return exceptionHandlerUtil.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");

		} catch (Exception e) {

			e.printStackTrace();
			return exceptionHandlerUtil.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");

		}

	}

}
