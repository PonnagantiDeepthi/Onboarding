package ug.daes.onboarding.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import ug.daes.DAESService;
import ug.daes.PKICoreServiceException;
import ug.daes.Result;

import ug.daes.onboarding.config.SentryClientExceptions;
import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.constant.ByteArrayToMultiPart;
import ug.daes.onboarding.constant.Constant;
import ug.daes.onboarding.dto.*;
import ug.daes.onboarding.enums.LogMessageType;
import ug.daes.onboarding.enums.ServiceNames;
import ug.daes.onboarding.enums.TransactionType;
import ug.daes.onboarding.exceptions.ExceptionHandlerUtil;
import ug.daes.onboarding.exceptions.OnBoardingServiceException;
import ug.daes.onboarding.model.*;
import ug.daes.onboarding.repository.*;
import ug.daes.onboarding.response.OnBoardingServiceResponse;
import ug.daes.onboarding.service.iface.ProposedFlowIface;
import ug.daes.onboarding.service.iface.SubscriberServiceIface;
import ug.daes.onboarding.service.iface.TemplateServiceIface;
import ug.daes.onboarding.util.AppUtil;
import ug.daes.onboarding.util.Utility;

import javax.sql.rowset.serial.SerialBlob;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import java.sql.Blob;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static ug.daes.onboarding.util.VersionComparatorThread.subscriber;

@Service
public class ProposedFlowImpl implements ProposedFlowIface {

	@Autowired
	SentryClientExceptions sentryClientExceptions;

	private static Logger logger = LoggerFactory.getLogger(ProposedFlowImpl.class);

	/**
	 * The Constant CLASS.
	 */
	final static String CLASS = "PraposedFlowImpl";

	@Value("${edms.localurl}")
	private String baselocalUrl;

	@Value("${edms.downloadurl}")
	private String edmsDwonlodUrl;

	@Value("${is.onboarding.fee}")
	boolean isOnboardingFee;

	@Value("${verify.photo}")
	private Boolean verifyPhoto;

	@Value("${age.validation.enabled}")
	private boolean ageValidationEnabled;

	@Value("${age.validation.min-age}")
	private int minAge;

	@Autowired
	private RestTemplate restTemplate;

	@Autowired
	OnboardingLivelinessRepository onboardingLivelinessRepository;

	@Autowired
	SubscriberRepoIface subscriberRepoIface;

	@Autowired
	SubscriberDeviceRepoIface subscriberDeviceRepoIface;

	@Autowired
	SubscriberDeviceRepoIface deviceRepoIface;

	@Autowired
	SubscriberStatusRepoIface statusRepoIface;

	@Autowired
	SubscriberFcmTokenRepoIface fcmTokenRepoIface;

	@Autowired
	ConsentHistoryRepo consentHistoryRepo;

	@Autowired
	SubscriberConsentsRepo subscriberConsentsRepo;

	@Autowired
	SubscriberOnboardingDataRepoIface onboardingDataRepoIface;

	@Autowired
	TemplateServiceIface templateServiceIface;

	@Autowired
	SubscriberCertificatesRepoIface subscriberCertificatesRepoIface;

	@Autowired
	SubscriberCertPinHistoryRepoIface subscriberCertPinHistoryRepoIface;

	@Autowired
	SubscriberCompleteDetailRepoIface subscriberCompleteDetailRepoIface;

	@Autowired
	SubscriberFcmTokenRepoIface subscriberFcmTokenRepoIface;
	@Autowired
	SubscriberStatusRepoIface subscriberStatusRepoIface;

	@Autowired
	TemporaryTableRepo temporaryTableRepo;

	@Autowired
	SubscriberDeviceHistoryRepoIface subscriberDeviceHistoryRepoIface;

	@Autowired
	LogModelServiceImpl logModelServiceImpl;

	@Autowired
	OnBoardingMethodRepoIface onBoardingMethodRepoIface;

	@Autowired
	OnboardingStepDetailsRepoIface onboardingStepsRepoIface;

	@Autowired
	EdmsServiceImpl edmsService;

	@Autowired
	SubscriberRaDataRepoIface raRepoIface;

	@Autowired
	KafkaSender mqSender;

	@Autowired
	SubscriberOnboardingDataRepoIface subscriberOnboardingDataRepoIface;

	@Autowired
	PhotoFeaturesRepo photoFeaturesRepo;

	@Autowired
	OnBoardingServiceException onBoardingServiceException;

	@Autowired
	MinioStorageServiceImpl minioStorageService;

	ObjectMapper objectMapper = new ObjectMapper();

	@Value("${signed.required.by.user}")
	private boolean signRequired;

	@Value(value = "${ind.api.sms}")
	private String indApiSMS;

	@Value("${au.log.url}")
	private String auditLogUrl;

	@Value("${extract.features}")
	private String exractFeatures;

	@Value("${find.details}")
	private String findDetails;

	@Autowired
	OnBoardingServiceResponse onBoardingServiceResponse;

	private final SubscriberServiceIface subscriberServiceIface;

	@Autowired
	ExceptionHandlerUtil exceptionHandlerUtil;

	@Autowired
	public ProposedFlowImpl(@Lazy SubscriberServiceIface subscriberServiceIface) {
		this.subscriberServiceIface = subscriberServiceIface;
	}

	@Override
	public ApiResponse saveDataTemporyTable(TemporaryTableDTO temporaryTableDTO) {
		try {
			if (Objects.isNull(temporaryTableDTO)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.temporary.table.dto.cannot.be.null");
			}
			if (temporaryTableDTO.getIdDocNumber() == null || temporaryTableDTO.getIdDocNumber().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.id.doc.number.cannot.be.null");
			}
			if (temporaryTableDTO.getStep() == 1) {
				logger.info("{}{} - Processing saveDataTemporyTable for step: {}", CLASS, Utility.getMethodName(),
						temporaryTableDTO.getStep());
				ApiResponse response = flag1method(temporaryTableDTO);
				if (!response.isSuccess()) {
					return exceptionHandlerUtil.createFailedResponseWithCustomMessage(response.getMessage(),
							response.getResult());
				}
				return exceptionHandlerUtil.createSuccessResponseWithCustomMessage(response.getMessage(),
						response.getResult());
			}
//            else if(temporaryTableDTO.getStep() == 2){
//                ApiResponse response =flag2method(temporaryTableDTO,livelinessVideo,selfie);
//                if(!response.isSuccess()){
//                    return AppUtil.createApiResponse(false,response.getMessage(),response.getResult());
//
//                }
//                return AppUtil.createApiResponse(true,response.getMessage(),response.getResult());
//            }
			else if (temporaryTableDTO.getStep() == 3) {
				logger.info("{}{} - saveDataTemporaryTable step: {}", CLASS, Utility.getMethodName(),
						temporaryTableDTO.getStep());
				ApiResponse response = flag3method(temporaryTableDTO);
				if (!response.isSuccess()) {
					return exceptionHandlerUtil.createFailedResponseWithCustomMessage(response.getMessage(),
							response.getResult());
				}
				return exceptionHandlerUtil.createSuccessResponseWithCustomMessage(response.getMessage(),
						response.getResult());
			} else if (temporaryTableDTO.getStep() == 4) {
				logger.info("{}{} - saveDataTemporaryTable step: {}", CLASS, Utility.getMethodName(),
						temporaryTableDTO.getStep());
				ApiResponse response = flag4method(temporaryTableDTO);
				if (!response.isSuccess()) {
					return exceptionHandlerUtil.createFailedResponseWithCustomMessage(response.getMessage(),
							response.getResult());
				}
				return exceptionHandlerUtil.createSuccessResponseWithCustomMessage(response.getMessage(),
						response.getResult());
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.step.not.found");
			}

		} catch (Exception e) {
			logger.error("{}{} - Exception in saveDataTemporyTable: {}", CLASS, Utility.getMethodName(),
					e.getMessage());
			e.printStackTrace();
			e.getCause();
			sentryClientExceptions.captureExceptions(e);
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	public ApiResponse flag1method(TemporaryTableDTO temporaryTableDTO) {
		try {
			SubscriberObDetails subscriberObData = temporaryTableDTO.getSubscriberObDataDTO();
			if (temporaryTableDTO.getDeviceId() == null || temporaryTableDTO.getDeviceId().isEmpty()) {
				logger.error("{}{} - Device cannot be null for idDocNumber: {}", CLASS, Utility.getMethodName(),
						temporaryTableDTO.getIdDocNumber());
				return exceptionHandlerUtil.createErrorResponse("api.error.application.info.not.found");
			} else if (Objects.isNull(subscriberObData)) {
				logger.error("{}{} - Subscriber Ob data cannot be null for idDocNumber: {}", CLASS,
						Utility.getMethodName(), temporaryTableDTO.getIdDocNumber());
				return exceptionHandlerUtil.createErrorResponse("api.error.temporary.table.dto.cannot.be.null");
			} else if (subscriberObData.getDocumentCode() == null || subscriberObData.getDocumentCode().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.doc.cant.be.null.or.empty");
			} else if (subscriberObData.getDocumentType() == null || subscriberObData.getDocumentType().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.doctype.cant.be.null.or.empty");
			} else if (subscriberObData.getNationality() == null || subscriberObData.getNationality().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.nationality.cant.be.null.or.empty");
			} else if (subscriberObData.getIssuingState() == null || subscriberObData.getIssuingState().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.issuing.state.cant.be.null.or.empty");
			} else if (subscriberObData.getSubscriberType() == null || subscriberObData.getSubscriberType().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.type.cant.be.null.or.empty");
			} else if (subscriberObData.getDateOfBirth() == null || subscriberObData.getDateOfBirth().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.date.of.birth.cant.be.null.or.empty");
			} else if (subscriberObData.getDateOfExpiry() == null || subscriberObData.getDateOfExpiry().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.date.of.expiry.cant.be.null.or.empty");
			} else if (subscriberObData.getGeoLocation() == null || subscriberObData.getGeoLocation().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.geolocation.cant.be.null.or.empty");
			}

			if (Objects.isNull(temporaryTableDTO.getSubscriberDeviceInfoDto())) {
				logger.info("{}{} - Subscriber Device info cannot be null : {}", CLASS, Utility.getMethodName(),
						temporaryTableDTO.getSubscriberDeviceInfoDto());
				return exceptionHandlerUtil.createErrorResponse("api.error.application.info.not.found");
			}

			if (temporaryTableDTO.getSubscriberDeviceInfoDto().getFcmToken() == null
					&& temporaryTableDTO.getSubscriberDeviceInfoDto().getFcmToken().isEmpty()) {
				logger.info("{}{} - FcmToken can't be null or empty for idDocNumber: {}", CLASS,
						Utility.getMethodName(), temporaryTableDTO.getIdDocNumber());
				return exceptionHandlerUtil.createErrorResponse("api.error.application.info.not.found");
			} else if (temporaryTableDTO.getSubscriberDeviceInfoDto().getDeviceId() == null
					&& temporaryTableDTO.getSubscriberDeviceInfoDto().getDeviceId().isEmpty()
					&& temporaryTableDTO.getSubscriberDeviceInfoDto().getOsName() == null
					&& temporaryTableDTO.getSubscriberDeviceInfoDto().getOsName().isEmpty()
					&& temporaryTableDTO.getSubscriberDeviceInfoDto().getAppVersion() == null
					&& temporaryTableDTO.getSubscriberDeviceInfoDto().getAppVersion().isEmpty()) {
				logger.error("{}{} - Application info can't be null or empty for idDocNumber: {}", CLASS,
						Utility.getMethodName(), temporaryTableDTO.getIdDocNumber());
				return exceptionHandlerUtil.createErrorResponse("api.error.application.info.not.found");

			}
			// SubscriberOnboardingData onboardingData = (SubscriberOnboardingData)
			// subscriberOnboardingDataRepoIface.findSubscriberByDocIdLatestRecord(temporaryTableDTO.getIdDocNumber());

			List<SubscriberOnboardingData> records = subscriberOnboardingDataRepoIface
					.findSubscriberByDocIdLatestRecord(temporaryTableDTO.getIdDocNumber());

			SubscriberOnboardingData onboardingData = records.isEmpty() ? null : records.get(0);

			Subscriber subscriber;
			if (onboardingData != null) {
				subscriber = subscriberRepoIface.findBysubscriberUid(onboardingData.getSubscriberUid());
			} else {
				subscriber = subscriberRepoIface.findbyDocumentNumber(temporaryTableDTO.getIdDocNumber());
			}

			if (subscriber != null) {
				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
//				SubscriberOnboardingData subscriberOnboardingData = (SubscriberOnboardingData) subscriberOnboardingDataRepoIface
//						.findLatestSubscriber(subscriber.getSubscriberUid());
//

				SubscriberOnboardingData subscriberOnboardingData = onboardingDataRepoIface
						.findLatestSubscriber(subscriber.getSubscriberUid()).stream().findFirst().orElse(null);

				if (subscriberOnboardingData == null) {
					logger.error("{}{} - Subscriber onboarding data cannot be null for idDocNumber: {}", CLASS,
							Utility.getMethodName(), temporaryTableDTO.getIdDocNumber());
					return exceptionHandlerUtil.createErrorResponse("api.error.temporaryTableDTO.cannot.be.null");
				}
				try {
					HttpHeaders head = new HttpHeaders();
					HttpEntity<Object> request = new HttpEntity<>(head);
					ResponseEntity<byte[]> resp = restTemplate.exchange(subscriberOnboardingData.getSelfieUri(),
							HttpMethod.GET, request, byte[].class);
					if (resp.getStatusCodeValue() == 200) {
					}
					String base64 = AppUtil.getBase64FromByteArr(resp.getBody());
					temporaryResponseDto.setSelfieImage(base64);

				} catch (Exception e) {
					e.printStackTrace();
					logger.error("{}{} - Subscriber getSelfieUrir Exception: {}", CLASS, Utility.getMethodName(), e);
					sentryClientExceptions.captureExceptions(e);
				}

				temporaryResponseDto.setSubscriber(subscriber);
				temporaryResponseDto.setExistingSubscriber(true);

				return exceptionHandlerUtil.createSuccessResponse(
						"api.response.it.seems.your.already.have.an.ugpass.account.kindly.log.in.to.access.your.account",
						temporaryResponseDto);
			}

//			SubscriberDevice subscriberDevice = (SubscriberDevice) subscriberDeviceRepoIface
//					.findDeviceDetailsById(temporaryTableDTO.getDeviceId());

			List<SubscriberDevice> devices = subscriberDeviceRepoIface
					.findDeviceDetailsById(temporaryTableDTO.getDeviceId());

			SubscriberDevice subscriberDevice = devices.isEmpty() ? null : devices.get(0);

			if (subscriberDevice != null && subscriberDevice.getDeviceStatus().equals("ACTIVE")) {
				logger.info("{}{} - Onboarded subscriber device details: {}", CLASS, Utility.getMethodName(),
						subscriberDevice);
				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
				temporaryResponseDto.setExistingSubscriberDevice(true);
				return exceptionHandlerUtil
						.createErrorResponse("api.error.device.is.already.registered.with.onboarded.user");

			}
			TemporaryTable temporaryTable = temporaryTableRepo.getbyidDocNumber(temporaryTableDTO.getIdDocNumber());

			TemporaryTable temporaryTableDevice = temporaryTableRepo.getByDevice(temporaryTableDTO.getDeviceId());
			List<OnboardingStepDetails> onboardingStepDetailslist = onboardingStepsRepoIface.getAllSteps();

			ObjectMapper objectMapper = new ObjectMapper();

//			ApiResponse validationResponse = validationsForName(temporaryTableDTO);
//
//			if (!validationResponse.isSuccess()) {
//				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(validationResponse.getMessage(),
//						validationResponse.getResult());
//			}

			// Convert DTO to JSON string
			String documentDetailsJson = objectMapper.writeValueAsString(temporaryTableDTO.getSubscriberObDataDTO());
			String deviceDetailsJson = objectMapper.writeValueAsString(temporaryTableDTO.getSubscriberDeviceInfoDto());

			if (temporaryTable != null) {
				if (temporaryTableDTO.getIdDocNumber().equals(temporaryTable.getIdDocNumber())
						&& temporaryTableDTO.getDeviceId().equals(temporaryTable.getDeviceId())) {
					logger.info("{}{} - Data already exists in temporary table for idDocNumber: {}", CLASS,
							Utility.getMethodName(), temporaryTable.getIdDocNumber());
					TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
					temporaryResponseDto.setIdDocNumber(temporaryTable.getIdDocNumber());
					temporaryResponseDto.setDeviceId(temporaryTable.getDeviceId());
					if (temporaryTable.getOptionalData1() != null && !temporaryTable.getOptionalData1().isEmpty()
							&& !temporaryTable.getOptionalData1().equals("0")) {
						temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());
					} else {
						temporaryResponseDto.setOptionalData1(temporaryTable.getIdDocNumber());
					}
					// temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());

					// Convert JSON string to DeviceInfoDTO
					SubscriberObDetails subscriberObDetails = objectMapper.readValue(temporaryTable.getStep1Data(),
							SubscriberObDetails.class);
					temporaryResponseDto.setSubscriberObDetails(subscriberObDetails);
					temporaryResponseDto.setStep1Status(temporaryTable.getStep1Status());

//                    temporaryResponseDto.setStep2Details(temporaryTable.getStep2Data());
					temporaryResponseDto.setStep2Status(temporaryTable.getStep2Status());
					temporaryResponseDto.setMobileNumber(temporaryTable.getStep3Data());
					temporaryResponseDto.setStep3Status(temporaryTable.getStep3Status());
					temporaryResponseDto.setEmailId(temporaryTable.getStep4Data());
					temporaryResponseDto.setStep4Status(temporaryTable.getStep4Status());
					temporaryResponseDto.setStep5Details(temporaryTable.getStep5Data());
					temporaryResponseDto.setStep5Status(temporaryTable.getStep5Status());
					temporaryResponseDto.setStepCompleted(temporaryTable.getStepCompleted());
					temporaryResponseDto.setNextStep(temporaryTable.getNextStep());
					temporaryResponseDto.setOnboardingStepDetails(onboardingStepDetailslist);
					temporaryResponseDto.setSelfieImage(temporaryTable.getSelfie());
					temporaryResponseDto.setDataInTemporaryTable(true);

					return exceptionHandlerUtil.createSuccessResponse("api.response.details.found",
							temporaryResponseDto);

				} else if (temporaryTableDevice == null
						&& temporaryTableDTO.getIdDocNumber().equals(temporaryTable.getIdDocNumber())
						&& !temporaryTableDTO.getDeviceId().equals(temporaryTable.getDeviceId())) {
					TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
					temporaryResponseDto.setNewDevice(true);
					logger.info(
							"{}{} - Data already exists in temporary table, coming with new device for idDocNumber: {}",
							CLASS, Utility.getMethodName(), temporaryTable.getIdDocNumber());
					return exceptionHandlerUtil.createSuccessResponse(
							"api.response.do.you.want.to.continue.on.this.new.device", temporaryResponseDto);
				}

			}

			if (temporaryTableDevice != null
					&& !temporaryTableDevice.getIdDocNumber().equals(temporaryTableDTO.getIdDocNumber())) {
				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
				temporaryResponseDto.setUsedDevice(true);
				return exceptionHandlerUtil.createSuccessResponse(
						"api.response.this.device.is.already.registered.with.different.details.use.the.same.document.to.proceed.or.delete.the.existing.data.and.try.again",
						temporaryResponseDto);
			}

			JsonNode jsonNode1 = objectMapper.readTree(documentDetailsJson);

			String subscriberType = jsonNode1.get("subscriberType").asText();
			if (subscriberType.equals("null") || subscriberType.isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.type");
			}

//			if (!subscriberType.equals(Constant.RESIDENT) && subscriberType != Constant.RESIDENT) {
//
//				if (temporaryTableDTO.getOptionalData1() != null && !temporaryTableDTO.getOptionalData1().isEmpty()
//						&& !temporaryTableDTO.getOptionalData1().equals("0")) {
//					int countOptional = temporaryTableRepo.getCountOfOptionalData(temporaryTableDTO.getOptionalData1());
//					System.out.println("OptionalData::::" + countOptional);
//					if (countOptional > 0) {
//						logger.info("{}{} - addSubscriberObData isOptionData1Present: Onboarding cannot be processed because the same national id already exists. Count: {}",
//								CLASS, Utility.getMethodName(), countOptional);
//						return exceptionHandlerUtil.createErrorResponse(
//								"api.error.onboarding.can.not.be.processed.because.the.same.national.id.already.exists");
//					}
//
//				} else {
//					logger.info("{}{} - addSubscriberObData isOptionData1 not Present, data: {}", CLASS, Utility.getMethodName(), temporaryTableDTO.getOptionalData1());
//					return exceptionHandlerUtil.createErrorResponse("api.error.optional.data.is.empty");
//				}
//
//				if (temporaryTableDTO.getOptionalData1() != null && !temporaryTableDTO.getOptionalData1().isEmpty()
//						&& !temporaryTableDTO.getOptionalData1().equals("0")) {
//					int count = isOptionData1Present(temporaryTableDTO.getOptionalData1());
//					if (count == 1) {
//						logger.info("{}{} - addSubscriberObData isOptionData1Present: Onboarding cannot be processed because the same national id already exists. Count: {}", CLASS, Utility.getMethodName(), count);
//						return exceptionHandlerUtil.createErrorResponse(
//								"api.error.onboarding.can.not.be.processed.because.the.same.national.id.already.exists");
//
//					}
//				} else {
//					logger.info("{}{} - addSubscriberObData isOptionData1 not Present, data: {}", CLASS, Utility.getMethodName(), temporaryTableDTO.getOptionalData1());
//					return exceptionHandlerUtil.createErrorResponse("api.error.optional.data.is.empty");
//				}
//			}

			/* kids on-boarding stop based on age */
			if (ageValidationEnabled) {
				LocalDate dob = AppUtil.parseToLocalDate(jsonNode1.get("dateOfBirth").asText());
				int age = AppUtil.calculateAge(dob);
				if (age < minAge) {
					return exceptionHandlerUtil.createErrorResponse(
							"api.error.you.cannot.onboard.Minimum.allowed.age.is.16.as.per.current.policy");
				}

			}

			TemporaryTable temporaryTable1 = new TemporaryTable();
			TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
			temporaryResponseDto.setSubscriberObDetails(temporaryTableDTO.getSubscriberObDataDTO());
			temporaryTable1.setDeviceInfo(deviceDetailsJson);
			temporaryResponseDto.setSubscriberDeviceInfoDto(temporaryTableDTO.getSubscriberDeviceInfoDto());
			temporaryTable1.setStep1Status("COMPLETED");
			temporaryResponseDto.setStep1Status("COMPLETED");
			temporaryTable1.setIdDocNumber(temporaryTableDTO.getIdDocNumber());
			temporaryResponseDto.setIdDocNumber(temporaryTable1.getIdDocNumber());
			temporaryTable1.setStep1Data(documentDetailsJson);
			temporaryTable1.setOptionalData1(temporaryTableDTO.getOptionalData1());
			temporaryResponseDto.setOptionalData1(temporaryTable1.getOptionalData1());
			temporaryTable1.setDeviceId(temporaryTableDTO.getDeviceId());
			temporaryResponseDto.setDeviceId(temporaryTable1.getDeviceId());
			temporaryTable1.setStepCompleted(temporaryTableDTO.getStep());
			temporaryResponseDto.setStepCompleted(temporaryTable1.getStepCompleted());

			ApiResponse res = nextStepDetails(temporaryTableDTO.getStep());
			String Response = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(res.getResult());
			OnboardingStepDetails responseDto = objectMapper.readValue(Response, OnboardingStepDetails.class);

			if (!res.isSuccess()) {
				temporaryTable1.setNextStep(temporaryTableDTO.getStep());
				temporaryResponseDto.setNextStep(temporaryTable1.getNextStep());
			}
			temporaryTable1.setNextStep(responseDto.getStepId());
			temporaryResponseDto.setNextStep(temporaryTable1.getNextStep());
			temporaryResponseDto.setOnboardingStepDetails(onboardingStepDetailslist);
			temporaryTable1.setCreatedOn(AppUtil.getDate());
			temporaryTable1.setUpdatedOn(AppUtil.getDate());
//			if(!isOnboardingFee) {
//				if (temporaryTableDTO.getNiraResponse() != null && temporaryTableDTO.getNiraResponse() != "") {
//
//					ObjectMapper ob = new ObjectMapper();
//					String s = ob.writeValueAsString(temporaryTableDTO.getNiraResponse());
//					Result r = DAESService.createSecureWireData(s);
//					temporaryTable1.setNiraResponse(new String(r.getResponse()));
//				}
//			}

			temporaryTableRepo.save(temporaryTable1);
			return exceptionHandlerUtil.createSuccessResponse(
					"api.response.details.of.step1.saved.successfully.in.temporary.table", temporaryResponseDto);

		} catch (Exception e) {
			logger.error("{}{} - Exception: {}", CLASS, Utility.getMethodName(), e.getMessage());
			e.printStackTrace();
			sentryClientExceptions.captureExceptions(e);
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@SuppressWarnings("null")
	public ApiResponse flag2method(TemporaryTableDTO temporaryTableDTO, MultipartFile livelinessVideo, String selfie) {
		try {
			if (Objects.isNull(temporaryTableDTO)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.temporary.table.dto.cannot.be.null");
			}
//			if (livelinessVideo == null || livelinessVideo.getSize() == 0 || livelinessVideo.isEmpty()) {
//				return exceptionHandlerUtil.createErrorResponse("api.error.liveness.video.invalid");
//			}
			if (selfie == null || selfie.isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.selfie.cannot.be.null");
			}

//			String contentType = livelinessVideo.getContentType();
//			logger.info("{}{} - LivelinessVideo contentType for idDocNumber: {}", CLASS, Utility.getMethodName(), contentType);
//
//			if (contentType == null && !contentType.startsWith("video/")) {
//				return exceptionHandlerUtil.createErrorResponse("api.error.vedio.content.type.isnot.mp4");
//			}
			TemporaryTable temporaryTable = temporaryTableRepo.getbyidDocNumber(temporaryTableDTO.getIdDocNumber());
			List<OnboardingStepDetails> onboardingStepDetailslist = onboardingStepsRepoIface.getAllSteps();
			if (onboardingStepDetailslist == null) {
				return exceptionHandlerUtil.createErrorResponse("api.error.onboarding.steps.cannot.be.null.or.empty");
			}
			if (Objects.isNull(temporaryTable)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.document.details.not.found");
			}
			if (temporaryTable.getStepCompleted() == 2 || temporaryTable.getSelfie() != null) {
				logger.info("{}{} - Data already exists in temporary table for idDocNumber: {}", CLASS,
						Utility.getMethodName(), temporaryTableDTO.getIdDocNumber());
				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
				temporaryResponseDto.setIdDocNumber(temporaryTable.getIdDocNumber());
				temporaryResponseDto.setDeviceId(temporaryTable.getDeviceId());
				if (temporaryTable.getOptionalData1() != null && !temporaryTable.getOptionalData1().isEmpty()
						&& !temporaryTable.getOptionalData1().equals("0")) {
					temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());
				} else {
					temporaryResponseDto.setOptionalData1(temporaryTable.getIdDocNumber());
				}
				// temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());

				// Convert JSON string to DeviceInfoDTO
				SubscriberObDetails subscriberObDetails = objectMapper.readValue(temporaryTable.getStep1Data(),
						SubscriberObDetails.class);
				temporaryResponseDto.setSubscriberObDetails(subscriberObDetails);
				temporaryResponseDto.setStep1Status(temporaryTable.getStep1Status());
				VideoDetailsDto videoDetailsDto = objectMapper.readValue(temporaryTable.getStep2Data(),
						VideoDetailsDto.class);
				temporaryResponseDto.setVideoDetailsDto(videoDetailsDto);
				temporaryResponseDto.setStep2Status(temporaryTable.getStep2Status());
				temporaryResponseDto.setMobileNumber(temporaryTable.getStep3Data());
				temporaryResponseDto.setStep3Status(temporaryTable.getStep3Status());
				temporaryResponseDto.setEmailId(temporaryTable.getStep4Data());
				temporaryResponseDto.setStep4Status(temporaryTable.getStep4Status());
				temporaryResponseDto.setStep5Details(temporaryTable.getStep5Data());
				temporaryResponseDto.setStep5Status(temporaryTable.getStep5Status());
				temporaryResponseDto.setStepCompleted(temporaryTable.getStepCompleted());
				temporaryResponseDto.setNextStep(temporaryTable.getNextStep());
				temporaryResponseDto.setOnboardingStepDetails(onboardingStepDetailslist);
				temporaryResponseDto.setDataInTemporaryTable(true);
				temporaryResponseDto.setSelfieImage(temporaryTable.getSelfie());
				return exceptionHandlerUtil.createSuccessResponse("api.response.details.found", temporaryResponseDto);
			}

			TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();

			String videoDetailsString = objectMapper.writeValueAsString(temporaryTableDTO.getVideoDetailsDto());

			temporaryTable.setStep2Status("COMPLETED");
			temporaryTable.setStep2Data(videoDetailsString);
			temporaryResponseDto.setStep2Status("COMPLETED");
			// temporaryTable.setLivelinessVideo(livelinessVideo.getBytes());
			temporaryTable.setSelfie(selfie);

			temporaryTable.setUpdatedOn(AppUtil.getDate());

			ApiResponse res = nextStepDetails(temporaryTableDTO.getStep());
			String Response = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(res.getResult());
			OnboardingStepDetails responseDto = objectMapper.readValue(Response, OnboardingStepDetails.class);

			if (!res.isSuccess()) {
				temporaryTable.setNextStep(temporaryTableDTO.getStep());
				temporaryResponseDto.setNextStep(temporaryTable.getNextStep());

			}
			temporaryTable.setNextStep(responseDto.getStepId());
			temporaryResponseDto.setNextStep(temporaryTable.getNextStep());

			temporaryTable.setStepCompleted(temporaryTableDTO.getStep());
			temporaryResponseDto.setStepCompleted(temporaryTable.getStepCompleted());

			temporaryResponseDto.setOnboardingStepDetails(onboardingStepDetailslist);

			temporaryResponseDto.setIdDocNumber(temporaryTable.getIdDocNumber());
			SubscriberObDetails subscriberObDetails = objectMapper.readValue(temporaryTable.getStep1Data(),
					SubscriberObDetails.class);
			temporaryResponseDto.setSubscriberObDetails(subscriberObDetails);
			temporaryResponseDto.setStep1Status(temporaryTable.getStep1Status());
			temporaryResponseDto.setDeviceId(temporaryTable.getDeviceId());
			if (temporaryTable.getOptionalData1() != null && !temporaryTable.getOptionalData1().isEmpty()
					&& !temporaryTable.getOptionalData1().equals("0")) {
				temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());
			} else {
				temporaryResponseDto.setOptionalData1(temporaryTable.getIdDocNumber());
			}
			// temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());
			temporaryResponseDto.setCreatedOn(temporaryTable.getCreatedOn());
			temporaryResponseDto.setCreatedOn(temporaryTable.getCreatedOn());
			temporaryResponseDto.setUpdatedOn(temporaryTable.getUpdatedOn());

			if (isOnboardingFee) {
				if (temporaryTableDTO.getNiraResponse() != null && temporaryTableDTO.getNiraResponse() != "") {
					ObjectMapper ob = new ObjectMapper();
					String s = ob.writeValueAsString(temporaryTableDTO.getNiraResponse());
					Result r = DAESService.createSecureWireData(s);
					temporaryTable.setNiraResponse(new String(r.getResponse()));
				}
			} else {
				if (temporaryTableDTO.getNiraResponse() != null && temporaryTableDTO.getNiraResponse() != "") {

					JsonNode root = objectMapper.readTree(temporaryTableDTO.getNiraResponse().toString());
					JsonNode dataNode = root.path("customerDetails").path("Result").path("Data");

					String passportNumber = dataNode.path("ActivePassport").path("DocumentNo").asText(null);

					String emiratesIdNumber = dataNode.path("ResidenceInfo").path("EmiratesIdNumber").asText(null);

					String emiratesIdDocumentNumber = dataNode.path("ResidenceInfo").path("DocumentNo").asText(null);

					List<String> reasons = subscriberRepoIface.findDuplicateReason(passportNumber, emiratesIdNumber,
							emiratesIdDocumentNumber);

					if (!reasons.isEmpty()) {
						switch (reasons.get(0)) {
						case "PASSPORT":
							// return error("Passport number already used");
							return exceptionHandlerUtil
									.createFailedResponseWithCustomMessage("Passport number already used", null);
						case "NATIONAL_ID":
							// return error("National ID number already used");
							return exceptionHandlerUtil
									.createFailedResponseWithCustomMessage("Emirates ID number already used", null);
						case "NATIONAL_ID_CARD":
							// return error("National ID card number already used");
							return exceptionHandlerUtil.createFailedResponseWithCustomMessage(
									"Emirates ID card number already used", null);
						}
					}

					temporaryTable.setNiraResponse(temporaryTableDTO.getNiraResponse().toString());
				}
			}

			temporaryTableRepo.save(temporaryTable);

			return exceptionHandlerUtil.createSuccessResponse(
					"api.response.details.of.step2.saved.successfully.temporary.table", temporaryResponseDto);
		} catch (Exception e) {
			logger.error("{}{} - Exception occurred in flag2method: {}", CLASS, Utility.getMethodName(), e.getMessage(),
					e);
			e.printStackTrace();
			sentryClientExceptions.captureExceptions(e);
			return ExceptionHandlerUtil.handleException(e);
		}

	}

	public ApiResponse flag3method(TemporaryTableDTO temporaryTableDTO) {
		try {
			if (Objects.isNull(temporaryTableDTO)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.temporary.table.dto.cannot.be.null");
			}
			if (temporaryTableDTO.getIdDocNumber() == null || temporaryTableDTO.getIdDocNumber().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.id.doc.number.cannot.be.null");
			}
			if (!StringUtils.hasText(temporaryTableDTO.getMobileNumber())) {
				return exceptionHandlerUtil.createErrorResponse("api.error.mobile.number.cant.be.empty");
			}
			Subscriber subscriber = subscriberRepoIface.findBymobileNumber(temporaryTableDTO.getMobileNumber());
			TemporaryTable temporaryTable = temporaryTableRepo.getbyidDocNumber(temporaryTableDTO.getIdDocNumber());
			TemporaryTable temporaryTableMobile = temporaryTableRepo
					.getByMobNumber(temporaryTableDTO.getMobileNumber());
			List<OnboardingStepDetails> onboardingStepDetailslist = onboardingStepsRepoIface.getAllSteps();
			if (subscriber != null) {
				logger.info("{}{} - details of onboarded subscriber: {}", CLASS, Utility.getMethodName(), subscriber);
				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
				temporaryResponseDto.setSubscriber(subscriber);
				temporaryResponseDto.setExistingSubscriber(true);
				return exceptionHandlerUtil.createErrorResponse("api.error.this.mobile.number.belongs.to.onboard.user");
			}
			if (Objects.isNull(temporaryTable)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.temporary.table.dto.cannot.be.null");
			}
			if (temporaryTable != null) {
				if (temporaryTableDTO.getIdDocNumber().equals(temporaryTable.getIdDocNumber())
						&& temporaryTableDTO.getMobileNumber().equals(temporaryTable.getStep3Data())) {
					logger.info("{}{} - Data already exists in temporary table for idDocNumber: {}", CLASS,
							Utility.getMethodName(), temporaryTableDTO.getIdDocNumber());
					TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
					temporaryResponseDto.setIdDocNumber(temporaryTable.getIdDocNumber());
					temporaryResponseDto.setDeviceId(temporaryTable.getDeviceId());
					if (temporaryTable.getOptionalData1() != null && !temporaryTable.getOptionalData1().isEmpty()
							&& !temporaryTable.getOptionalData1().equals("0")) {
						temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());
					} else {
						temporaryResponseDto.setOptionalData1(temporaryTable.getIdDocNumber());
					}
					// temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());
					ObjectMapper objectMapper = new ObjectMapper();

					SubscriberObDetails subscriberObDetails = objectMapper.readValue(temporaryTable.getStep1Data(),
							SubscriberObDetails.class);
					temporaryResponseDto.setSubscriberObDetails(subscriberObDetails);
					temporaryResponseDto.setStep1Status(temporaryTable.getStep1Status());
					temporaryResponseDto.setStep2Status(temporaryTable.getStep2Status());
					temporaryResponseDto.setMobileNumber(temporaryTable.getStep3Data());
					temporaryResponseDto.setStep3Status(temporaryTable.getStep3Status());
					temporaryResponseDto.setEmailId(temporaryTable.getStep4Data());
					temporaryResponseDto.setStep4Status(temporaryTable.getStep4Status());
					temporaryResponseDto.setStep5Details(temporaryTable.getStep5Data());
					temporaryResponseDto.setStep4Status(temporaryTable.getStep5Status());
					temporaryResponseDto.setStepCompleted(temporaryTable.getStepCompleted());
					temporaryResponseDto.setNextStep(temporaryTable.getNextStep());
					temporaryResponseDto.setOnboardingStepDetails(onboardingStepDetailslist);
					temporaryResponseDto.setDataInTemporaryTable(true);
					temporaryResponseDto.setSelfieImage(temporaryTable.getSelfie());
					return exceptionHandlerUtil.createSuccessResponse("api.response.details.found",
							temporaryResponseDto);
				}
			}
			if (temporaryTableMobile == null
					&& temporaryTable.getIdDocNumber().equals(temporaryTableDTO.getIdDocNumber())
					&& (temporaryTable.getStep3Data() != null
							&& !temporaryTable.getStep3Data().equals(temporaryTableDTO.getMobileNumber()))) {
				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
				temporaryResponseDto.setNewMobileNumber(true);
				logger.info("{}{} - Do you want to continue with this new Mobile number for idDocNumber: {}", CLASS,
						Utility.getMethodName(), temporaryTable.getIdDocNumber());
				return exceptionHandlerUtil.createErrorResponseWithResult(
						"api.error.do.you.want.to.continue.with.this.new.mobile.number", temporaryResponseDto);
			}

			if (temporaryTableMobile != null
					&& !temporaryTableMobile.getIdDocNumber().equals(temporaryTableDTO.getIdDocNumber())) {
				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
				temporaryResponseDto.setUsedMobNumber(true);
				logger.info("{}{} - This Mobile number belongs to another onboarding user for idDocNumber: {}", CLASS,
						Utility.getMethodName(), temporaryTable.getIdDocNumber());
				return exceptionHandlerUtil.createErrorResponseWithResult(
						"api.error.this.mobile.number.belongs.to.onboard.user", temporaryResponseDto);
			}

			TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
			temporaryResponseDto.setIdDocNumber(temporaryTable.getIdDocNumber());
			ObjectMapper objectmp = new ObjectMapper();

			SubscriberObDetails subscriberObDetails = objectmp.readValue(temporaryTable.getStep1Data(),
					SubscriberObDetails.class);
			temporaryResponseDto.setSubscriberObDetails(subscriberObDetails);
			temporaryResponseDto.setStep1Status(temporaryTable.getStep1Status());
			temporaryResponseDto.setStep2Status(temporaryTable.getStep2Status());
			temporaryResponseDto.setDeviceId(temporaryTable.getDeviceId());
			if (temporaryTable.getOptionalData1() != null && !temporaryTable.getOptionalData1().isEmpty()
					&& !temporaryTable.getOptionalData1().equals("0")) {
				temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());
			} else {
				temporaryResponseDto.setOptionalData1(temporaryTable.getIdDocNumber());
			}
			// temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());
			temporaryResponseDto.setCreatedOn(temporaryTable.getCreatedOn());
			temporaryResponseDto.setUpdatedOn(temporaryTable.getUpdatedOn());
			temporaryTable.setStep3Data(temporaryTableDTO.getMobileNumber());
			temporaryResponseDto.setMobileNumber(temporaryTableDTO.getMobileNumber());
			temporaryTable.setStepCompleted(temporaryTableDTO.getStep());
			temporaryResponseDto.setStepCompleted(temporaryTableDTO.getStep());
			temporaryTable.setStep3Status("COMPLETED");
			temporaryResponseDto.setStep3Status("COMPLETED");
			ApiResponse res = nextStepDetails(temporaryTableDTO.getStep());
			String Response = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(res.getResult());
			OnboardingStepDetails responseDto = objectMapper.readValue(Response, OnboardingStepDetails.class);
			if (!res.isSuccess()) {
				temporaryTable.setNextStep(temporaryTableDTO.getStep());
				temporaryResponseDto.setNextStep(temporaryTableDTO.getStep());
			}
			temporaryTable.setNextStep(responseDto.getStepId());
			temporaryResponseDto.setNextStep(responseDto.getStepId());
			temporaryResponseDto.setOnboardingStepDetails(onboardingStepDetailslist);
			temporaryTableRepo.save(temporaryTable);
			return exceptionHandlerUtil.createSuccessResponse(
					"api.response.details.of.step3.saved.successfully.in.temporary.table", temporaryResponseDto);
		} catch (Exception e) {
			logger.error("{}{} - Exception: {}", CLASS, Utility.getMethodName(), e.getMessage());
			e.printStackTrace();
			sentryClientExceptions.captureExceptions(e);
			return ExceptionHandlerUtil.handleException(e);

		}

	}

	public ApiResponse flag4method(TemporaryTableDTO temporaryTableDTO) {
		try {
			if (Objects.isNull(temporaryTableDTO)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.temporary.table.dto.cannot.be.null");
			}
			if (temporaryTableDTO.getIdDocNumber() == null || temporaryTableDTO.getIdDocNumber().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.id.doc.number.cannot.be.null");
			}
			if (!StringUtils.hasText(temporaryTableDTO.getEmailId())) {
				return exceptionHandlerUtil.createErrorResponse("api.error.email.id.cant.be.empty");
			}
			logger.info("{}{} - flag4method EmailID: {}", CLASS, Utility.getMethodName(),
					temporaryTableDTO.getEmailId());

			temporaryTableDTO.setEmailId(temporaryTableDTO.getEmailId().toLowerCase());
			Subscriber subscriber = subscriberRepoIface.findByemailId(temporaryTableDTO.getEmailId());
			TemporaryTable temporaryTableEmail = temporaryTableRepo.getByEmail(temporaryTableDTO.getEmailId());
			TemporaryTable temporaryTable = temporaryTableRepo.getbyidDocNumber(temporaryTableDTO.getIdDocNumber());
			List<OnboardingStepDetails> onboardingStepDetailslist = onboardingStepsRepoIface.getAllSteps();
			if (subscriber != null) {
				logger.info("{}{} - details of onboarded subscriber: {}", CLASS, Utility.getMethodName(), subscriber);
				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
				temporaryResponseDto.setSubscriber(subscriber);
				temporaryResponseDto.setExistingSubscriber(true);
				return exceptionHandlerUtil.createErrorResponse("api.error.this.email.id.belongs.to.onboard.user");
			}
			if (temporaryTable == null) {
				return exceptionHandlerUtil.createErrorResponse("api.error.details.not.found");
			}
			if (temporaryTable.getStep3Data() == null || temporaryTable.getStep3Data().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.mobile.number.not.found");
			}
			if (temporaryTable != null) {
				if (temporaryTableDTO.getIdDocNumber().equals(temporaryTable.getIdDocNumber())
						&& temporaryTableDTO.getEmailId().equals(temporaryTable.getStep4Data())) {
					logger.info("{}{} - Data already exists in temporary table for idDocNumber: {}", CLASS,
							Utility.getMethodName(), temporaryTableDTO.getIdDocNumber());
					TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
					temporaryResponseDto.setIdDocNumber(temporaryTable.getIdDocNumber());
					temporaryResponseDto.setDeviceId(temporaryTable.getDeviceId());
					if (temporaryTable.getOptionalData1() != null && !temporaryTable.getOptionalData1().isEmpty()
							&& !temporaryTable.getOptionalData1().equals("0")) {
						temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());
					} else {
						temporaryResponseDto.setOptionalData1(temporaryTable.getIdDocNumber());
					}
					// temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());
					SubscriberObDetails subscriberObDetails = objectMapper.readValue(temporaryTable.getStep1Data(),
							SubscriberObDetails.class);
					temporaryResponseDto.setSubscriberObDetails(subscriberObDetails);
					temporaryResponseDto.setStep1Status(temporaryTable.getStep1Status());
					temporaryResponseDto.setStep2Status(temporaryTable.getStep2Status());
					temporaryResponseDto.setMobileNumber(temporaryTable.getStep3Data());
					temporaryResponseDto.setStep3Status(temporaryTable.getStep3Status());
					temporaryResponseDto.setEmailId(temporaryTable.getStep4Data());
					temporaryResponseDto.setStep4Status(temporaryTable.getStep4Status());
					temporaryResponseDto.setStep5Details(temporaryTable.getStep5Data());
					temporaryResponseDto.setStep4Status(temporaryTable.getStep5Status());
					temporaryResponseDto.setStepCompleted(temporaryTable.getStepCompleted());
					temporaryResponseDto.setNextStep(temporaryTable.getNextStep());
					temporaryResponseDto.setOnboardingStepDetails(onboardingStepDetailslist);
					temporaryResponseDto.setSelfieImage(temporaryTable.getSelfie());
					temporaryResponseDto.setDataInTemporaryTable(true);
					return exceptionHandlerUtil.createSuccessResponse("api.response.details.found",
							temporaryResponseDto);
				}
			}
			if (temporaryTableEmail == null
					&& temporaryTable.getIdDocNumber().equals(temporaryTableDTO.getIdDocNumber())
					&& (temporaryTable.getStep4Data() != null
							&& !temporaryTable.getStep4Data().equals(temporaryTableDTO.getEmailId()))) {
				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
				temporaryResponseDto.setNewEmailId(true);
				logger.info("{}{} - Do you want to continue with this new Email id for idDocNumber: {}", CLASS,
						Utility.getMethodName(), temporaryTable.getIdDocNumber());
				return exceptionHandlerUtil.createErrorResponseWithResult(
						"api.error.do.you.want.to.continue.with.this.new.email.id", temporaryResponseDto);
			}

			if (temporaryTableEmail != null
					&& !temporaryTableEmail.getIdDocNumber().equals(temporaryTableDTO.getIdDocNumber())) {
				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
				temporaryResponseDto.setUsedEmail(true);
				logger.info("{}{} - This email ID belongs to another onboarding user for idDocNumber: {}", CLASS,
						Utility.getMethodName(), temporaryTable.getIdDocNumber());
				return exceptionHandlerUtil.createErrorResponseWithResult(
						"api.error.this.email.id.belongs.to.onboard.user", temporaryResponseDto);
			}

			TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
			temporaryResponseDto.setIdDocNumber(temporaryTable.getIdDocNumber());
			SubscriberObDetails subscriberObDetails = objectMapper.readValue(temporaryTable.getStep1Data(),
					SubscriberObDetails.class);
			temporaryResponseDto.setSubscriberObDetails(subscriberObDetails);
			temporaryResponseDto.setStep1Status(temporaryTable.getStep1Status());
			temporaryResponseDto.setStep2Status(temporaryTable.getStep2Status());
			temporaryResponseDto.setDeviceId(temporaryTable.getDeviceId());
			if (temporaryTable.getOptionalData1() != null && !temporaryTable.getOptionalData1().isEmpty()
					&& !temporaryTable.getOptionalData1().equals("0")) {
				temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());
			} else {
				temporaryResponseDto.setOptionalData1(temporaryTable.getIdDocNumber());
			}
			// temporaryResponseDto.setOptionalData1(temporaryTable.getOptionalData1());
			temporaryResponseDto.setCreatedOn(temporaryTable.getCreatedOn());
			temporaryResponseDto.setCreatedOn(temporaryTable.getCreatedOn());
			temporaryResponseDto.setUpdatedOn(temporaryTable.getUpdatedOn());
			temporaryResponseDto.setMobileNumber(temporaryTable.getStep3Data());
			temporaryResponseDto.setStep3Status(temporaryTable.getStep3Status());

			temporaryTable.setStep4Data(temporaryTableDTO.getEmailId());
			temporaryResponseDto.setEmailId(temporaryTable.getStep4Data());

			temporaryTable.setStep4Status("COMPLETED");
			temporaryResponseDto.setStep4Status("COMPLETED");

			temporaryTable.setStepCompleted(temporaryTableDTO.getStep());
			temporaryResponseDto.setStepCompleted(temporaryTable.getStepCompleted());
			ApiResponse res = nextStepDetails(temporaryTableDTO.getStep());
			if (!res.isSuccess()) {
				temporaryTable.setNextStep(temporaryTableDTO.getStep());
				temporaryResponseDto.setNextStep(temporaryTableDTO.getStep());
			} else {
				String Response = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(res.getResult());
				OnboardingStepDetails responseDto = objectMapper.readValue(Response, OnboardingStepDetails.class);
				temporaryTable.setNextStep(responseDto.getStepId());
				temporaryResponseDto.setNextStep(temporaryTable.getNextStep());
			}
			temporaryResponseDto.setOnboardingStepDetails(onboardingStepDetailslist);
			temporaryTableRepo.save(temporaryTable);
			return exceptionHandlerUtil.createSuccessResponse(
					"api.response.details.of.step4.saved.successfully.in.temporary.table", temporaryResponseDto);
		} catch (Exception e) {
			logger.error("{}{} - Exception: {}", CLASS, Utility.getMethodName(), e.getMessage());
			e.printStackTrace();
			sentryClientExceptions.captureExceptions(e);
			return ExceptionHandlerUtil.handleException(e);

		}

	}

	public static SubscriberOnboardingData findLatestOnboardedSub(
			List<SubscriberOnboardingData> subscriberOnboardingData) {
		Date[] dates = new Date[subscriberOnboardingData.size() - 1];
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

	@Override
	public ApiResponse submitObData(String idDocumentNumber) {
		try {
			logger.info("{}{} - Received request to submit data for idDocumentNumber: {}", CLASS,
					Utility.getMethodName(), idDocumentNumber);
			if (!StringUtils.hasText(idDocumentNumber) || "null".equalsIgnoreCase(idDocumentNumber)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.id.document.number.cannot.be.null");
			}
			TemporaryTable temporaryTable = temporaryTableRepo.getbyidDocNumber(idDocumentNumber);
			int countOfValues = onboardingStepsRepoIface.getNoOfOnboardingSteps();
			if (Objects.isNull(temporaryTable)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.no.data.found.for.given.id.doc.number");
			} else if (temporaryTable.getStepCompleted() != countOfValues) {
				return exceptionHandlerUtil.createErrorResponse("api.error.please.complete.all.steps");
			}

			String featuresBase64 = null;

			if (verifyPhoto) {
				ApiResponse responseOfFaceVerification = verifyFaceFeatures(temporaryTable.getSelfie());
				if (!responseOfFaceVerification.isSuccess()) {
					return AppUtil.createApiResponse(false, responseOfFaceVerification.getMessage(),
							responseOfFaceVerification.getResult());
				}
				featuresBase64 = responseOfFaceVerification.getResult().toString();
			}

			String step1Json = temporaryTable.getStep1Data();

			String deviceInfo = temporaryTable.getDeviceInfo();

			MobileOTPDto mobileOTPDto = new MobileOTPDto();
			JsonNode documentDetailsJson = objectMapper.readTree(step1Json);
			JsonNode deviceDetailsJson = objectMapper.readTree(deviceInfo);

			mobileOTPDto.setSubscriberName(documentDetailsJson.get("subscriberName").asText());
			mobileOTPDto.setDeviceId(temporaryTable.getDeviceId());
			mobileOTPDto.setSubscriberMobileNumber(temporaryTable.getStep3Data());
			mobileOTPDto.setSubscriberEmail(temporaryTable.getStep4Data());
			mobileOTPDto.setFcmToken(deviceDetailsJson.get("fcmToken").asText());
			mobileOTPDto.setOtpStatus(true);
			mobileOTPDto.setOsName(deviceDetailsJson.get("osName").asText());
			mobileOTPDto.setOsVersion(deviceDetailsJson.get("osVersion").asText());
			mobileOTPDto.setAppVersion(deviceDetailsJson.get("appVersion").asText());
			mobileOTPDto.setDeviceInfo(deviceDetailsJson.get("deviceInfo").asText());
			mobileOTPDto.setIdDocNumber(idDocumentNumber);

			ApiResponse response = subscriberServiceIface.saveSubscribersData(mobileOTPDto);

			if (!response.isSuccess()) {
				logger.info(CLASS + " submitObData  saveSubscribersData: 2  " + response);
				subscriberServiceIface.deleteRecord("", mobileOTPDto.getSubscriberEmail());
				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(response.getMessage(),
						response.getResult());
			}

			SubscriberRegisterResponseDTO responseDTO = (SubscriberRegisterResponseDTO) response.getResult();

			// Access the suID field
			String suID = responseDTO.getSuID();

			// saving data into photo features

			if (verifyPhoto) {
				byte[] decodedData = Base64.getDecoder().decode(featuresBase64);

				Blob blob = new SerialBlob(decodedData);

				PhotoFeatures photoFeatures = new PhotoFeatures();
				photoFeatures.setPhotoFeatures(blob);
				photoFeatures.setSuid(suID);
				photoFeatures.setCreatedOn(AppUtil.getDate());
				photoFeatures.setUpdatedOn(AppUtil.getDate());
				photoFeaturesRepo.save(photoFeatures);

			}

			String step2Json = temporaryTable.getStep2Data();

			// for saving video in EDMS or minIO
//			FileUploadDTO fileUploadDTO = populateFileUploadDTO(step2Json, suID);
//			MultipartFile multipartFile = convertToMultipartFile(temporaryTable.getLivelinessVideo(), "file",
//					"filename.mp4", "video/mp4");
			// for saving video in EDMS
//			CompletableFuture<ApiResponse> videoResponse = edmsService.saveFileToEdms(multipartFile, "video",
//					fileUploadDTO);

			// for saving video in to minIO
//			CompletableFuture<ApiResponse> videoResponse = minioStorageService.saveFileToMinio(multipartFile, "video",
//					fileUploadDTO);
//			ApiResponse apiResponse = videoResponse.get();
//			//System.out.println(" videoResponse ::"+videoResponse);
//			if (!apiResponse.isSuccess()) {
//				logger.info("{}{} - videoResponse: {}", CLASS, Utility.getMethodName(), apiResponse);
//				subscriberServiceIface.deleteRecord("", mobileOTPDto.getSubscriberEmail());
//				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(apiResponse.getMessage(),
//						apiResponse.getResult());
//			}

			// for saving video in EDMS
//			FileUploadDTO fileUploadDTO = populateFileUploadDTO(step2Json, suID);
//			MultipartFile multipartFile = convertToMultipartFile(temporaryTable.getLivelinessVideo(), "file",
//					"filename.mp4", "video/mp4");
//			CompletableFuture<ApiResponse> videoResponse = edmsService.saveFileToEdms(multipartFile, "video",
//					fileUploadDTO);
//			ApiResponse apiResponse = videoResponse.get();
//			System.out.println(" videoResponse ::"+videoResponse);
//			if (!apiResponse.isSuccess()) {
//				logger.info("{}{} - videoResponse: {}", CLASS, Utility.getMethodName(), apiResponse);
//				subscriberServiceIface.deleteRecord("", mobileOTPDto.getSubscriberEmail());
//				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(apiResponse.getMessage(),
//						apiResponse.getResult());
//			}

			SubscriberObRequestDTO subscriberObRequestDTO = createSubscriberObRequestDTO(suID, documentDetailsJson,
					temporaryTable, idDocumentNumber);
			ApiResponse res = subscriberServiceIface.addSubscriberObData(subscriberObRequestDTO);

			if (!res.isSuccess()) {
				ApiResponse deleteResponse = subscriberServiceIface.deleteRecord("", mobileOTPDto.getSubscriberEmail());
				logger.info("{}{} - deleteResponse: {}", CLASS, Utility.getMethodName(), deleteResponse);
				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(res.getMessage(), res.getResult());
			}

			int deleteValue = temporaryTableRepo.deleteRecordByIdDocumentNumber(idDocumentNumber);
			if (deleteValue != 1) {
				logger.info("{}{} - deleteValue: {}", CLASS, Utility.getMethodName(), deleteValue);
				return exceptionHandlerUtil.createErrorResponse("api.error.Record.not.deleted.from.temporary.table");
			}
			return exceptionHandlerUtil.createSuccessResponseWithCustomMessage(res.getMessage(), res.getResult());
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("{}{} - submitObData Exception : {}", CLASS, Utility.getMethodName(), e);
			sentryClientExceptions.captureExceptions(e);
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	public SubscriberObRequestDTO createSubscriberObRequestDTO(String suID, JsonNode documentDetailsJson,
			TemporaryTable temporaryTable, String idDocumentNumber) {
		try {
			SubscriberObRequestDTO subscriberObRequestDTO = new SubscriberObRequestDTO();
			subscriberObRequestDTO.setSuID(suID);
			subscriberObRequestDTO.setOnboardingMethod(documentDetailsJson.get("onboardingMethod").asText());
			subscriberObRequestDTO.setTemplateId(documentDetailsJson.get("templateID").asInt());
			subscriberObRequestDTO.setSubscriberType(documentDetailsJson.get("subscriberType").asText());
			subscriberObRequestDTO.setNiraResponse(temporaryTable.getNiraResponse());

			SubscriberObData subscriberObData = new SubscriberObData();
			subscriberObData.setDateOfBirth(documentDetailsJson.get("dateOfBirth").asText());
			subscriberObData.setDateOfExpiry(documentDetailsJson.get("dateOfExpiry").asText());
			subscriberObData.setNationality(documentDetailsJson.get("nationality").asText());
			subscriberObData.setGender(documentDetailsJson.get("gender").asText());
			subscriberObData.setPrimaryIdentifier(documentDetailsJson.get("primaryIdentifier").asText());
			subscriberObData.setSecondaryIdentifier(documentDetailsJson.get("secondaryIdentifier").asText());
			subscriberObData.setDocumentType(documentDetailsJson.get("documentType").asText());
			subscriberObData.setDocumentCode(documentDetailsJson.get("documentCode").asText());
			subscriberObData.setOptionalData1(documentDetailsJson.get("optionalData1").asText());
			subscriberObData.setOptionalData2(documentDetailsJson.get("optionalData2").asText());
			subscriberObData.setDocumentNumber(idDocumentNumber);
			subscriberObData.setIssuingState(documentDetailsJson.get("issuingState").asText());
			subscriberObData.setSubscriberSelfie(temporaryTable.getSelfie());
			subscriberObData.setGeoLocation(documentDetailsJson.get("geoLocation").asText());
			subscriberObData.setRemarks(documentDetailsJson.get("remarks").asText());
			subscriberObData.setSubscriberUniqueId(suID);

			subscriberObData.setNiraResponse(temporaryTable.getNiraResponse());
			subscriberObRequestDTO.setSubscriberData(subscriberObData);

			return subscriberObRequestDTO;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	public FileUploadDTO populateFileUploadDTO(String step2Json, String suID) throws IOException {
		FileUploadDTO videoUploadReq = objectMapper.readValue(step2Json, FileUploadDTO.class);

		FileUploadDTO fileUploadDTO = new FileUploadDTO();
		fileUploadDTO.setSubscriberUid(suID);
		fileUploadDTO.setRecordedTime(videoUploadReq.getRecordedTime());
		fileUploadDTO.setRecordedGeoLocation(videoUploadReq.getRecordedGeoLocation());
		fileUploadDTO.setVerificationFirst(videoUploadReq.getVerificationFirst());
		fileUploadDTO.setVerificationSecond(videoUploadReq.getVerificationSecond());
		fileUploadDTO.setVerificationThird(videoUploadReq.getVerificationThird());
		fileUploadDTO.setTypeOfService(videoUploadReq.getTypeOfService());
		return fileUploadDTO;
	}

	private MultipartFile convertToMultipartFile(byte[] byteArray, String paramName, String fileName,
			String contentType) {
		return new ByteArrayToMultiPart(byteArray, // byte array of file contents
				paramName, // parameter name for the file
				fileName, // original filename
				contentType // content type of the file
		);
	}

	public String generateSubscriberUniqueId() {
		UUID uuid = UUID.randomUUID();
		logger.info(CLASS + "Generate Subscriber UniqueId {}", uuid.toString());
		return uuid.toString();
	}

	int isOptionData1Present(String optionalData1) {
		int optionalDataCount = onboardingDataRepoIface.getOptionalData1(optionalData1);
		return optionalDataCount;
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
			// System.out.println("json => " + json);
			Result checksumResult = DAESService.addChecksumToTransaction(json);
			String push = new String(checksumResult.getResponse());
			LogModelDTO log = objectMapper.readValue(push, LogModelDTO.class);
			mqSender.send(log);
		} catch (Exception e) {
			logger.error("Set LogModel Exception {}", e.getMessage());
			e.printStackTrace();
		}
	}

	private String getTimeStampString() throws ParseException {
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		return f.format(new Date());
	}

	public ApiResponse nextStepDetails(int currentStepId) {
		try {
			int countNoOfSteps = onboardingStepsRepoIface.getNoOfOnboardingSteps();
			if (countNoOfSteps == currentStepId) {
				return AppUtil.createApiResponse(false, "Last Step", countNoOfSteps);
			}
			OnboardingStepDetails onboardingSteps = onboardingStepsRepoIface.getStepDetails(currentStepId + 1);
			return exceptionHandlerUtil.createSuccessResponse("api.response.next.step.details", onboardingSteps);
		} catch (Exception e) {
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}

	}

	@Override
	public ApiResponse updateRecord(UpdateTemporaryTableDto updateTemporaryTableDto) {
		try {
			logger.info("{}{} - Request for update record: {}", CLASS, Utility.getMethodName(),
					updateTemporaryTableDto);
			if (updateTemporaryTableDto.getIdDocNumber() == null
					|| updateTemporaryTableDto.getIdDocNumber().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.id.doc.number.cannot.be.null");
			}
			List<OnboardingStepDetails> onboardingStepDetailsList = onboardingStepsRepoIface.getAllSteps();
			if (updateTemporaryTableDto.getSubscriberDeviceInfoDto() != null) {
				TemporaryTable temporaryTable1 = temporaryTableRepo
						.getbyidDocNumber(updateTemporaryTableDto.getIdDocNumber());

				if (Objects.isNull(temporaryTable1)) {
					return exceptionHandlerUtil
							.createErrorResponse("api.error.no.record.found.for.given.document.id.number");
				}

				String deviceDetailsJson = objectMapper
						.writeValueAsString(updateTemporaryTableDto.getSubscriberDeviceInfoDto());
				JsonNode jsonNode = objectMapper.readTree(deviceDetailsJson);
				String deviceId = jsonNode.get("deviceId").asText();

				temporaryTable1.setDeviceId(deviceId);
				temporaryTable1.setDeviceInfo(deviceDetailsJson);

				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
				temporaryResponseDto.setIdDocNumber(temporaryTable1.getIdDocNumber());
				SubscriberObDetails subscriberObDetails = objectMapper.readValue(temporaryTable1.getStep1Data(),
						SubscriberObDetails.class);
				temporaryResponseDto.setSubscriberObDetails(subscriberObDetails);
				temporaryResponseDto.setStep1Status(temporaryTable1.getStep1Status());
				temporaryResponseDto.setStep2Status(temporaryTable1.getStep2Status());
				temporaryResponseDto.setDeviceId(temporaryTable1.getDeviceId());
				SubscriberDeviceInfoDto subscriberDeviceInfoDto = objectMapper
						.readValue(temporaryTable1.getDeviceInfo(), SubscriberDeviceInfoDto.class);
				temporaryResponseDto.setSubscriberDeviceInfoDto(subscriberDeviceInfoDto);
				temporaryResponseDto.setOptionalData1(temporaryTable1.getOptionalData1());
				temporaryResponseDto.setCreatedOn(temporaryTable1.getCreatedOn());
				temporaryResponseDto.setCreatedOn(temporaryTable1.getCreatedOn());
				temporaryResponseDto.setUpdatedOn(temporaryTable1.getUpdatedOn());
				temporaryResponseDto.setMobileNumber(temporaryTable1.getStep3Data());
				temporaryResponseDto.setStep3Status(temporaryTable1.getStep3Status());

				temporaryResponseDto.setStepCompleted(temporaryTable1.getStepCompleted());
				temporaryResponseDto.setNextStep(temporaryTable1.getNextStep());
				temporaryResponseDto.setStep4Status(temporaryTable1.getStep4Status());
				temporaryResponseDto.setEmailId(temporaryTable1.getStep4Data());
				temporaryResponseDto.setOnboardingStepDetails(onboardingStepDetailsList);

				temporaryTableRepo.save(temporaryTable1);
				return exceptionHandlerUtil.createSuccessResponse("api.response.device.updated.successfully",
						temporaryResponseDto);
			} else if (updateTemporaryTableDto.getMobileNumber() != null
					|| !updateTemporaryTableDto.getMobileNumber().isEmpty()) {
				TemporaryTable temporaryTable1 = temporaryTableRepo
						.getbyidDocNumber(updateTemporaryTableDto.getIdDocNumber());

				if (Objects.isNull(temporaryTable1)) {
					return exceptionHandlerUtil
							.createErrorResponse("api.error.no.record.found.for.given.document.id.number");
				}
				temporaryTable1.setStep3Data(updateTemporaryTableDto.getMobileNumber());

				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
				temporaryResponseDto.setIdDocNumber(temporaryTable1.getIdDocNumber());
				SubscriberObDetails subscriberObDetails = objectMapper.readValue(temporaryTable1.getStep1Data(),
						SubscriberObDetails.class);
				temporaryResponseDto.setSubscriberObDetails(subscriberObDetails);
				temporaryResponseDto.setStep1Status(temporaryTable1.getStep1Status());
				temporaryResponseDto.setStep2Status(temporaryTable1.getStep2Status());
				temporaryResponseDto.setDeviceId(temporaryTable1.getDeviceId());
				SubscriberDeviceInfoDto subscriberDeviceInfoDto = objectMapper
						.readValue(temporaryTable1.getDeviceInfo(), SubscriberDeviceInfoDto.class);
				temporaryResponseDto.setSubscriberDeviceInfoDto(subscriberDeviceInfoDto);
				temporaryResponseDto.setOptionalData1(temporaryTable1.getOptionalData1());
				temporaryResponseDto.setCreatedOn(temporaryTable1.getCreatedOn());
				temporaryResponseDto.setCreatedOn(temporaryTable1.getCreatedOn());
				temporaryResponseDto.setUpdatedOn(temporaryTable1.getUpdatedOn());
				temporaryResponseDto.setMobileNumber(temporaryTable1.getStep3Data());
				temporaryResponseDto.setStep3Status(temporaryTable1.getStep3Status());

				temporaryResponseDto.setStepCompleted(temporaryTable1.getStepCompleted());
				temporaryResponseDto.setStep4Status(temporaryTable1.getStep4Status());
				temporaryResponseDto.setEmailId(temporaryTable1.getStep4Data());
				temporaryResponseDto.setOnboardingStepDetails(onboardingStepDetailsList);

				temporaryTableRepo.save(temporaryTable1);
				return exceptionHandlerUtil.createSuccessResponse("api.response.mobile.number.updated.successfully",
						temporaryResponseDto);
			} else if (updateTemporaryTableDto.getEmailId() != null
					|| !updateTemporaryTableDto.getEmailId().isEmpty()) {
				TemporaryTable temporaryTable1 = temporaryTableRepo
						.getbyidDocNumber(updateTemporaryTableDto.getIdDocNumber());

				if (Objects.isNull(temporaryTable1)) {
					return exceptionHandlerUtil
							.createErrorResponse("api.error.no.record.found.for.given.document.id.number");

				}
				temporaryTable1.setStep4Data(updateTemporaryTableDto.getEmailId());

				TemporaryResponseDto temporaryResponseDto = new TemporaryResponseDto();
				temporaryResponseDto.setIdDocNumber(temporaryTable1.getIdDocNumber());
				SubscriberObDetails subscriberObDetails = objectMapper.readValue(temporaryTable1.getStep1Data(),
						SubscriberObDetails.class);
				temporaryResponseDto.setSubscriberObDetails(subscriberObDetails);
				temporaryResponseDto.setStep1Status(temporaryTable1.getStep1Status());
				temporaryResponseDto.setStep2Status(temporaryTable1.getStep2Status());
				temporaryResponseDto.setDeviceId(temporaryTable1.getDeviceId());
				SubscriberDeviceInfoDto subscriberDeviceInfoDto = objectMapper
						.readValue(temporaryTable1.getDeviceInfo(), SubscriberDeviceInfoDto.class);
				temporaryResponseDto.setSubscriberDeviceInfoDto(subscriberDeviceInfoDto);
				temporaryResponseDto.setOptionalData1(temporaryTable1.getOptionalData1());
				temporaryResponseDto.setCreatedOn(temporaryTable1.getCreatedOn());
				temporaryResponseDto.setCreatedOn(temporaryTable1.getCreatedOn());
				temporaryResponseDto.setUpdatedOn(temporaryTable1.getUpdatedOn());
				temporaryResponseDto.setMobileNumber(temporaryTable1.getStep3Data());
				temporaryResponseDto.setStep3Status(temporaryTable1.getStep3Status());

				temporaryResponseDto.setStepCompleted(temporaryTable1.getStepCompleted());
				temporaryResponseDto.setStep4Status(temporaryTable1.getStep4Status());
				temporaryResponseDto.setEmailId(temporaryTable1.getStep4Data());
				temporaryResponseDto.setOnboardingStepDetails(onboardingStepDetailsList);

				temporaryTableRepo.save(temporaryTable1);
				return exceptionHandlerUtil.createSuccessResponse("api.response.email.id.updated.successfully",
						temporaryResponseDto);
			}
			return exceptionHandlerUtil.createErrorResponse("api.error.update.record.type.not.valid");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("{}{} - Exception for update record: {}", CLASS, Utility.getMethodName(), e);
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	@Override
	public ApiResponse deleteRecord(UpdateTemporaryTableDto updateTemporaryTableDto) {
		try {
			if (temporaryTableRepo.getByMobNumber(updateTemporaryTableDto.getMobileNumber()) != null) {
				Optional<TemporaryTable> temporaryTable = Optional
						.ofNullable(temporaryTableRepo.getByMobNumber(updateTemporaryTableDto.getMobileNumber()));
				if (!temporaryTable.isPresent()) {
					return exceptionHandlerUtil
							.createErrorResponse("api.error.there.is.no.record.with.this.mobile.number");
				}

				int a = temporaryTableRepo.deleteRecord(updateTemporaryTableDto.getMobileNumber(), null, null);
				if (a != 1) {
					return exceptionHandlerUtil
							.createErrorResponse("api.error.temporary.table.record.is.not.deleted.by.using.device.id");
				}
				return exceptionHandlerUtil.successResponse("api.response.temporary.table.record.deleted.successfully");

			}
			if (temporaryTableRepo.getByEmail(updateTemporaryTableDto.getEmailId()) != null) {
				Optional<TemporaryTable> temporaryTable = Optional
						.ofNullable(temporaryTableRepo.getByEmail(updateTemporaryTableDto.getMobileNumber()));
				if (!temporaryTable.isPresent()) {
					return exceptionHandlerUtil.createErrorResponse("api.error.there.is.no.record.with.this.email.id");

				}
				int a = temporaryTableRepo.deleteRecord(null, updateTemporaryTableDto.getEmailId(), null);
				if (a != 1) {
					return exceptionHandlerUtil
							.createErrorResponse("api.error.temporary.table.record.is.not.deleted.by.using.device.id");

				}
				return exceptionHandlerUtil.successResponse("api.response.temporary.table.record.deleted.successfully");

			}

			if (temporaryTableRepo.getByDevice(updateTemporaryTableDto.getDeviceId()) != null) {
				Optional<TemporaryTable> temporaryTable = Optional
						.ofNullable(temporaryTableRepo.getByDevice(updateTemporaryTableDto.getDeviceId()));
				if (!temporaryTable.isPresent()) {
					return exceptionHandlerUtil.createErrorResponse("api.error.There.is.no.record.with.this.device.id");

				}
				int a = temporaryTableRepo.deleteRecord(null, null, updateTemporaryTableDto.getDeviceId());
				if (a != 1) {

					return exceptionHandlerUtil
							.createErrorResponse("api.error.temporary.table.record.is.not.deleted.by.using.device.id");
				}

				return exceptionHandlerUtil.successResponse("api.response.temporary.table.record.deleted.successfully");
			}

			return exceptionHandlerUtil.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");

		} catch (Exception e) {
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	public static File convert(MultipartFile file) {
		System.out.println("TOMCAT_HOME_PATH ::" + System.getProperty("catalina.home"));
		String tomcatBasePath = System.getProperty("catalina.home");
		// Create a File object representing the folder
		File folder = new File(tomcatBasePath, "ObTempFiles");
		File convFile = new File(folder.getAbsolutePath() + File.separator + file.getOriginalFilename());
		// Check if the folder already exists
		if (folder.exists()) {
			System.out.println("Folder already exists. PATH ::" + folder.getAbsolutePath());
			try {
				convFile.createNewFile();
				FileOutputStream fos = new FileOutputStream(convFile);
				fos.write(file.getBytes());
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return convFile;
		} else {
			// Create the folder
			boolean created = folder.mkdir();
			// Check if the folder creation was successful
			if (created) {
				System.out.println("Folder created successfully. PATH ::" + folder.getAbsolutePath());
			} else {
				System.out.println("Failed to create the folder.");
			}
			try {
				convFile.createNewFile();
				FileOutputStream fos = new FileOutputStream(convFile);
				fos.write(file.getBytes());
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return convFile;
		}
	}

	@Override
	public ApiResponse saveStep2Details(TemporaryTableDTO temporaryTableDTO, MultipartFile livelinessVideo,
			String selfie) {
		try {
			if (Objects.isNull(temporaryTableDTO)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.temporary.table.dto.cannot.be.null");
			}
			if (temporaryTableDTO.getIdDocNumber() == null || temporaryTableDTO.getIdDocNumber().isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.id.doc.number.cannot.be.null");
			}
			if (temporaryTableDTO.getStep() == 2) {
				ApiResponse response = flag2method(temporaryTableDTO, livelinessVideo, selfie);
				if (!response.isSuccess()) {
					return AppUtil.createApiResponse(false, response.getMessage(), response.getResult());
				}
				return AppUtil.createApiResponse(true, response.getMessage(), response.getResult());
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.step.not.found");
			}
		} catch (Exception e) {
			logger.error("{}{} - getOnBoardingSteps Exception: {}", CLASS, Utility.getMethodName(), e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Scheduled(cron = "0 0 0 * * ?")
	@Override
	public void deleteOldRecords() {
		try {
			logger.info("{}{} - deleteOldRecords corn job Started: {}", CLASS, Utility.getMethodName());
			List<TemporaryTable> records = temporaryTableRepo.findAll();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			LocalDateTime now = LocalDateTime.now();
			for (TemporaryTable record : records) {
				if (record.getUpdatedOn() != null) {
					LocalDateTime updatedOn = LocalDateTime.parse(record.getUpdatedOn(), formatter);
					LocalDateTime threshold = now.minusHours(24);
					if (updatedOn.isBefore(threshold) || updatedOn.isEqual(threshold)) {
						temporaryTableRepo.deleteRecordByIdDocumentNumber(record.getIdDocNumber());
					}
				}
			}
			logger.info("{}{} - deleteOldRecords record deleted: {}", CLASS, Utility.getMethodName());
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("{}{} - deleteOldRecords Exception: {}", CLASS, Utility.getMethodName(), e.getMessage());
		}
	}

	// Api to save face features into database in photo_features table
	@Override
	public ApiResponse getAllSubscriberExtractFeatures() {
		try {
			List<SubscriberOnboardingData> subscriberOnboardingDataList = subscriberOnboardingDataRepoIface
					.getAllSelfies();

			for (SubscriberOnboardingData subscriberOnboardingData : subscriberOnboardingDataList) {
				try {
					ApiResponse response = externalEdmsApi(subscriberOnboardingData.getSelfieUri());

					ApiResponse response1 = extractFeatchersPython(response.getResult().toString());
					if (!response1.isSuccess()) {
						return exceptionHandlerUtil
								.createErrorResponse("api.error.response.from.facefeature.python.api.is.negative");
					}
					String featuresBase64 = response1.getResult().toString();

					byte[] decodedData = Base64.getDecoder().decode(featuresBase64);

					Blob blob = new SerialBlob(decodedData);

//                        byte[] featuresByte = featuresBase64.getBytes();

					PhotoFeatures photoFeatures = new PhotoFeatures();

					photoFeatures.setSuid(subscriberOnboardingData.getSubscriberUid());
					photoFeatures.setPhotoFeatures(blob);
					photoFeatures.setCreatedOn(AppUtil.getDate());
					photoFeatures.setUpdatedOn(AppUtil.getDate());
					photoFeaturesRepo.save(photoFeatures);

				} catch (Exception e) {
					logger.error(CLASS + " getAllSubscriberExtractFeatures Exception {}", e.getMessage());
					e.printStackTrace();
					return ExceptionHandlerUtil.handleException(e);

				}
			}
			return exceptionHandlerUtil
					.createSuccessResponseWithCustomMessage("All features are extracted and saved in db", null);
		} catch (Exception e) {
			logger.error(CLASS + " getAllSubscriberExtractFeatures Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	// Python api called to fetch face featues
	public ApiResponse extractFeatchersPython(String subscriberPhoto) {
		try {
			HttpHeaders headers = new HttpHeaders();

			extractFeatureInputDto extractFeatureInputDto = new extractFeatureInputDto();
			extractFeatureInputDto.setSubscriberPhoto(subscriberPhoto);
			HttpEntity<Object> request = new HttpEntity<>(extractFeatureInputDto, headers);

			ResponseEntity<ApiResponse> response = restTemplate.exchange(exractFeatures, HttpMethod.POST, request,
					ApiResponse.class);

			if (!response.getBody().isSuccess()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.Extract.feature.python.api.failed");
			}
			return exceptionHandlerUtil.createSuccessResponse("api.response.features.extracted.successfully",
					response.getBody().getResult());

		} catch (Exception e) {
			logger.error(CLASS + " extractFeatchersPython Exception {}", e.getMessage());
			e.printStackTrace();
			return exceptionHandlerUtil.handleHttpException(e);
		}

	}

	// api to fetch subscriber selfie base 64 from edms
	public ApiResponse externalEdmsApi(String edmsUrl) {
		try {
			try {
				logger.info("{}{} - externalEdmsApi request: {}", CLASS, Utility.getMethodName(), edmsUrl);
				HttpHeaders head = new HttpHeaders();
				HttpEntity<Object> request = new HttpEntity<>(head);
				ResponseEntity<byte[]> resp = restTemplate.exchange(edmsUrl, HttpMethod.GET, request, byte[].class);
				if (resp.getStatusCodeValue() == 200) {
					String selfieBase64 = AppUtil.getBase64FromByteArr(resp.getBody());
					return exceptionHandlerUtil.createSuccessResponse("api.response.edms.selfie", selfieBase64);
				} else {
					return exceptionHandlerUtil.createErrorResponse("api.error.edms.selfie.fetchednot.fetched");
				}
			} catch (Exception e) {
				e.printStackTrace();
				return exceptionHandlerUtil.handleHttpException(e);
			}
		} catch (Exception e) {
			logger.error("{}{} - externalEdmsApi Exception: {}", CLASS, Utility.getMethodName(), e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	public ApiResponse verifyFaceFeatures(String selfieBase64) {
		try {
			ApiResponse response1 = findDetails(selfieBase64);
			if (!response1.isSuccess()) {
				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(response1.getMessage(),
						response1.getResult());
			}
			return exceptionHandlerUtil.createSuccessResponseWithCustomMessage(response1.getMessage(),
					response1.getResult());

		} catch (Exception e) {
			logger.error("{}{} - Exception occurred in verifyFaceFeatures: {}", CLASS, Utility.getMethodName(),
					e.getMessage(), e);
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	public ApiResponse findDetails(String subscriberPhoto) {
		try {
			HttpHeaders headers = new HttpHeaders();
			extractFeatureInputDto extractFeatureInputDto = new extractFeatureInputDto();
			extractFeatureInputDto.setImage(subscriberPhoto);
			HttpEntity<Object> request = new HttpEntity<>(extractFeatureInputDto, headers);

			ResponseEntity<ApiResponse> response = restTemplate.exchange(findDetails, HttpMethod.POST, request,
					ApiResponse.class);

			if (!response.getBody().isSuccess()) {
				return exceptionHandlerUtil.createFailedResponseWithCustomMessage(response.getBody().getMessage(),
						response.getBody().getResult());
			}
			return exceptionHandlerUtil.createSuccessResponseWithCustomMessage(response.getBody().getMessage(),
					response.getBody().getResult());
		} catch (Exception e) {
			logger.error("{}{} - Exception occurred in extractFeatchersPython: {}", CLASS, Utility.getMethodName(),
					e.getMessage(), e);
			e.printStackTrace();
			return exceptionHandlerUtil.handleHttpException(e);
		}

	}

	@Override
	public ApiResponse encriptedString(TemporaryTableDTO temporaryTableDTO) {
		try {
			Result r = DAESService.createSecureWireData(temporaryTableDTO.getNiraResponse().toString());
			String decryptedString = new String(r.getResponse());
			Result result = DAESService.decryptSecureWireData(decryptedString);
			String ss = new String(result.getResponse());
			return exceptionHandlerUtil.createSuccessResponseWithCustomMessage("Success", ss);

		} catch (Exception e) {
			logger.error(CLASS + " extractFeatchersPython Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Override
	public ApiResponse niraResponse(String docNumber) {
		try {

			// SubscriberOnboardingData temporaryTable = (SubscriberOnboardingData)
			// onboardingDataRepoIface.findLatestSubscriber(docNumber);

			SubscriberOnboardingData temporaryTable = onboardingDataRepoIface
					.findLatestSubscriber(subscriber.getSubscriberUid()).stream().findFirst().orElse(null);

			if (temporaryTable != null) {
				Result result = DAESService.decryptSecureWireData(temporaryTable.getNiraResponse());
				String s = new String(result.getResponse());
				return AppUtil.createApiResponse(true, "Success", s);
			} else {
				return AppUtil.createApiResponse(false, "no record found", null);
			}

		} catch (Exception e) {
			e.printStackTrace();
			return AppUtil.createApiResponse(false, "falied", null);
		}
	}

	public ApiResponse validationsForName(TemporaryTableDTO temporaryTableDTO) {
		try {
			System.out.println(" IN validationsForName");
			SubscriberObDetails subscriber = temporaryTableDTO.getSubscriberObDataDTO();
			System.out.println(" IN subscriber.getSubscriberName():" + subscriber.getSubscriberName());
			if (!subscriber.getSubscriberName().matches("^[a-zA-Z\\s]+$")) {
				System.out.println(" IN subscriber.getSubscriberName():" + subscriber.getSubscriberName());
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.name.not.valid");
			}

			if (!subscriber.getNationality().matches("^[a-zA-Z]+$")) {
				System.out.println(" IN subscriber.getNationality():" + subscriber.getNationality());
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.nationality.not.valid");
			}
			if (!subscriber.getGender().matches("^[a-zA-Z]+$")) {
				System.out.println(" IN subscriber.getGender():" + subscriber.getGender());
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.gender.not.valid");
			}
			// Parse the issueDate and dateOfExpiry to LocalDate
			LocalDate expiryDate = AppUtil
					.parseDateWithoutTime(temporaryTableDTO.getSubscriberObDataDTO().getDateOfExpiry());
			LocalDate currentDate = LocalDate.now(); // Current date for validation

			// Check if the expiry date is after the current date and after the issue date
			if (expiryDate.isBefore(currentDate)) {
				logger.error("validationsForName(): Expiry date cannot be before today.");
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.date.of.expiry.not.valid");
			}

			LocalDate dateOfbirth = AppUtil
					.parseDateWithoutTime(temporaryTableDTO.getSubscriberObDataDTO().getDateOfBirth());

			// Check if the expiry date is after the current date and after the issue date
			if (dateOfbirth.isAfter(currentDate)) {
				logger.error("validationsForName(): Date of Birth cannot be after today.");
				return exceptionHandlerUtil.createErrorResponse("api.error.subscriber.date.of.birth.not.valid");
			}
			return exceptionHandlerUtil.createSuccessResponse("api.response.all.validations.are.passed", null);
		} catch (NullPointerException npe) {
			logger.error(CLASS + " validations() NullPointerException: {}", npe.getMessage(), npe);
			return ExceptionHandlerUtil.handleException(npe);
		} catch (DateTimeParseException dtpe) {
			logger.error(CLASS + " validations() DateTimeParseException: {}", dtpe.getMessage(), dtpe);
			return ExceptionHandlerUtil.handleException(dtpe);
		} catch (IllegalArgumentException iae) {
			logger.error(CLASS + " validations() IllegalArgumentException: {}", iae.getMessage(), iae);
			return ExceptionHandlerUtil.handleException(iae);
		} catch (Exception e) {
			logger.error(CLASS + " validations() Unexpected Exception: {}", e.getMessage(), e);
			sentryClientExceptions.captureExceptions(e);
			return ExceptionHandlerUtil.handleException(e);
		}
	}

}
