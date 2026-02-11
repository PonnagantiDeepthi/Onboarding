package ug.daes.onboarding.service.impl;

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
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.constant.Constant;
import ug.daes.onboarding.constant.DeviceUpdatePolicy;
import ug.daes.onboarding.dto.*;
import ug.daes.onboarding.exceptions.ExceptionHandlerUtil;
import ug.daes.onboarding.model.*;
import ug.daes.onboarding.repository.*;
import ug.daes.onboarding.service.iface.DeviceUpdateIface;
import ug.daes.onboarding.service.iface.PolicyIface;
import ug.daes.onboarding.service.iface.SubscriberServiceIface;
import ug.daes.onboarding.service.iface.TemplateServiceIface;
import ug.daes.onboarding.util.AppUtil;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static ug.daes.onboarding.service.impl.SubscriberServiceImpl.findLatestOnboardedSub;

@Service
public class DeviceUpdateImpl implements DeviceUpdateIface {

	private static Logger logger = LoggerFactory.getLogger(DeviceUpdateImpl.class);

	final static String CLASS = "DeviceUpdateImpl";

	@Autowired
	SubscriberRepoIface subscriberRepoIface;

	@Autowired
	SubscriberDeviceRepoIface deviceRepoIface;

	@Autowired
	MessageSource messageSource;

	@Autowired
	LogModelServiceImpl logModelServiceImpl;

	@Autowired
	SubscriberOnboardingDataRepoIface onboardingDataRepoIface;
	@Autowired
	TemplateServiceIface templateServiceIface;
	@Autowired
	SubscriberStatusRepoIface statusRepoIface;
	@Autowired
	SubscriberCertificatesRepoIface subscriberCertificatesRepoIface;
	@Autowired
	SubscriberCertPinHistoryRepoIface subscriberCertPinHistoryRepoIface;

	@Autowired
	SubscriberFcmTokenRepoIface fcmTokenRepoIface;

	@Autowired
	SubscriberDeviceHistoryRepoIface subscriberDeviceHistoryRepoIface;

	@Autowired
	PolicyIface policyIface;

	private final SubscriberServiceIface subscriberServiceIface;

	@Autowired
	public DeviceUpdateImpl(@Lazy SubscriberServiceIface subscriberServiceIface) {
		this.subscriberServiceIface = subscriberServiceIface;
	}

	@Autowired
	DevicePolicyRepository devicePolicyRepository;

	@Value("${device.update.min.policy}")
	private long minhour;
	
	@Value("${is.onboarding.fee}")
	private boolean selfieRequired;

	@Value("${device.update.max.policy}")
	private long maxhour;

	@Autowired
	ExceptionHandlerUtil exceptionHandlerUtil;

	@Override
	public void updateSubscriberDeviceAndHistory(SubscriberDevice oldDevice, String newDeviceUid) {
		// save to subscriber device history
		try {
			logger.info(CLASS + "updateSubscriberDeviceAndHistory oldDevice and newDeviceUid {}, {}", oldDevice,
					newDeviceUid);

			SubscriberDeviceHistory subscriberDeviceHistory = new SubscriberDeviceHistory();
			subscriberDeviceHistory.setDeviceUid(oldDevice.getDeviceUid());
			subscriberDeviceHistory.setDeviceStatus(Constant.DEVICE_STATUS_DISABLED);
			subscriberDeviceHistory.setSubscriberUid(oldDevice.getSubscriberUid());
			subscriberDeviceHistory.setCreatedDate(AppUtil.getDate());
			subscriberDeviceHistory.setUpdatedDate(AppUtil.getDate());
			subscriberDeviceHistoryRepoIface.save(subscriberDeviceHistory);

			oldDevice.setDeviceUid(newDeviceUid);
			oldDevice.setDeviceStatus(Constant.DEVICE_STATUS_ACTIVE);
			// oldDevice.setCreatedDate(AppUtil.getDate());
			oldDevice.setUpdatedDate(AppUtil.getDate());
			deviceRepoIface.save(oldDevice);

		} catch (JDBCConnectionException | ConstraintViolationException | DataException | LockAcquisitionException
				| PessimisticLockException | QueryTimeoutException | SQLGrammarException | GenericJDBCException ex) {
			ex.printStackTrace();
			logger.error(CLASS + "updateSubscriberDeviceAndHistory Exception {}", ex.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + "updateSubscriberDeviceAndHistory Exception {}", e.getMessage());
		}

	}

	@Override
	public ApiResponse activateNewDevice(DeviceInfo deviceInfo, MobileOTPDto mobileOTPDto) {
		try {
			
			if (Objects.isNull(deviceInfo) && Objects.isNull(mobileOTPDto)) {
				return exceptionHandlerUtil.createErrorResponse("api.error.device.info.and.mobile.otp.dtos.cant.null");
			}
			int countDevice = subscriberRepoIface.countSubscriberDevice(deviceInfo.getDeviceId());
			logger.info(CLASS + "checkValidationForSubscriber countDevice {}, DeviceId {} ", countDevice,
					deviceInfo.getDeviceId());
			int countMobile = subscriberRepoIface.countSubscriberMobile(mobileOTPDto.getSubscriberMobileNumber());
			logger.info(CLASS + "checkValidationForSubscriber countMobile {} , SubscriberMobileNumber {} ", countMobile,
					mobileOTPDto.getSubscriberMobileNumber());
			int countEmail = subscriberRepoIface
					.countSubscriberEmailId(mobileOTPDto.getSubscriberEmail().toLowerCase());
			logger.info(CLASS + "checkValidationForSubscriber countEmail {}, SubscriberEmail {} ", countEmail,
					mobileOTPDto.getSubscriberEmail().toLowerCase());
			Subscriber subscriber = subscriberRepoIface.getSubscriberUidByEmailAndMobile(
					mobileOTPDto.getSubscriberEmail(), mobileOTPDto.getSubscriberMobileNumber());

			logger.info(CLASS + " activateNewDevice countDevice and countEmail and countMobile {}, {}, {}", countDevice,
					countEmail, countMobile);
			Date startTime = new Date();
			if (countDevice == 0) {

				if (countEmail == 1 && countMobile == 1) {
					SubscriberDevice device = deviceRepoIface.getSubscriber(subscriber.getSubscriberUid());
					if (device.getDeviceStatus().equals(Constant.DEVICE_STATUS_ACTIVE)) {

						// save subscriber device in history table
						updateSubscriberDeviceAndHistory(device, deviceInfo.getDeviceId());
						// save preSubscriberDevice in

						Subscriber subscriber2 = subscriberRepoIface.findBysubscriberUid(subscriber.getSubscriberUid());

						Date endTime = new Date();
						String device_info = mobileOTPDto.getOsName() + " | " + deviceInfo.getOsVersion() + " | "
								+ deviceInfo.getAppVersion() + " | " + mobileOTPDto.getDeviceInfo();
						String message = "DEVICE_UPDATE | " + deviceInfo.getDeviceId() + "|" + device.getDeviceUid()
								+ "|" + AppUtil.getDate() + "|" + device_info;

						logger.info(CLASS + " activateNewDevice device change mongo {} ", message);

						logModelServiceImpl.setLogModelDTO(true, device.getSubscriberUid(), null, "OTHER", null,
								message, startTime, endTime, null);

						if (subscriber2 != null) {
							subscriber2.setAppVersion(deviceInfo.getAppVersion());
							subscriber2.setOsVersion(deviceInfo.getOsVersion());
							subscriber2.setOsName(mobileOTPDto.getOsName());
							subscriber2.setDeviceInfo(mobileOTPDto.getDeviceInfo());
							subscriberRepoIface.save(subscriber2);
						}
						// update fcm token
						updateFcmToken(subscriber.getSubscriberUid(), mobileOTPDto.getFcmToken());

						return exceptionHandlerUtil
								.successResponse("api.response.services.now.accessible.on.this.device.welcome.back");

					} else {
						if (device.getDeviceStatus().equals(Constant.DEVICE_STATUS_DISABLED)) {
							updateSubscriberDeviceAndHistory(device, deviceInfo.getDeviceId());

							Subscriber subscriber2 = subscriberRepoIface
									.findBysubscriberUid(subscriber.getSubscriberUid());

							Date endTime = new Date();
							String device_info = mobileOTPDto.getOsName() + " | " + deviceInfo.getOsVersion() + " | "
									+ deviceInfo.getAppVersion() + " | " + mobileOTPDto.getDeviceInfo();
							String message = "DEVICE_UPDATE | " + deviceInfo.getDeviceId() + "|" + device.getDeviceUid()
									+ "|" + AppUtil.getDate() + "|" + device_info;

							logger.info(CLASS + " activateNewDevice device change mongo {} ", message);

							logModelServiceImpl.setLogModelDTO(true, device.getSubscriberUid(), null, "OTHER", null,
									message, startTime, endTime, null);

							if (subscriber2 != null) {
								subscriber2.setAppVersion(deviceInfo.getAppVersion());
								subscriber2.setOsVersion(deviceInfo.getOsVersion());
								subscriber2.setOsName(mobileOTPDto.getOsName());
								subscriber2.setDeviceInfo(mobileOTPDto.getDeviceInfo());
								subscriberRepoIface.save(subscriber2);
							}
							// update fcm token
							updateFcmToken(subscriber.getSubscriberUid(), mobileOTPDto.getFcmToken());

							return exceptionHandlerUtil.successResponse(
									"api.response.services.now.accessible.on.this.device.welcome.back");

						}
					}

				}
			} else {
				if (countEmail == 1 && countMobile == 1) {
					SubscriberDevice device = deviceRepoIface.getSubscriber(subscriber.getSubscriberUid());
					System.out.println("device " + device);

					if (device.getDeviceStatus().equals(Constant.DEVICE_STATUS_ACTIVE)) {

						// save subscriber device in history table
						updateSubscriberDeviceAndHistory(device, deviceInfo.getDeviceId());

						// save preSubscriberDevice in
						Subscriber subscriber2 = subscriberRepoIface.findBysubscriberUid(subscriber.getSubscriberUid());

						Date endTime = new Date();
						String device_info = mobileOTPDto.getOsName() + " | " + deviceInfo.getOsVersion() + " | "
								+ deviceInfo.getAppVersion() + " | " + mobileOTPDto.getDeviceInfo();
						String message = "DEVICE_UPDATE | " + deviceInfo.getDeviceId() + "|" + device.getDeviceUid()
								+ "|" + AppUtil.getDate() + "|" + device_info;

						logger.info(CLASS + " activateNewDevice device change mongo {} ", message);

						logModelServiceImpl.setLogModelDTO(true, device.getSubscriberUid(), null, "OTHER", null,
								message, startTime, endTime, null);

						if (subscriber2 != null) {
							subscriber2.setAppVersion(deviceInfo.getAppVersion());
							subscriber2.setOsVersion(deviceInfo.getOsVersion());
							subscriber2.setOsName(mobileOTPDto.getOsName());
							subscriber2.setDeviceInfo(mobileOTPDto.getDeviceInfo());
							subscriberRepoIface.save(subscriber2);
						}
						// update fcm token
						updateFcmToken(subscriber.getSubscriberUid(), mobileOTPDto.getFcmToken());

						return exceptionHandlerUtil
								.successResponse("api.response.services.now.accessible.on.this.device.welcome.back");

					} else {
						if (device.getDeviceStatus().equals(Constant.DEVICE_STATUS_DISABLED)) {
							updateSubscriberDeviceAndHistory(device, deviceInfo.getDeviceId());

							Subscriber subscriber2 = subscriberRepoIface
									.findBysubscriberUid(subscriber.getSubscriberUid());

							Date endTime = new Date();
							String device_info = mobileOTPDto.getOsName() + " | " + deviceInfo.getOsVersion() + " | "
									+ deviceInfo.getAppVersion() + " | " + mobileOTPDto.getDeviceInfo();
							String message = "DEVICE_UPDATE | " + deviceInfo.getDeviceId() + "|" + device.getDeviceUid()
									+ "|" + AppUtil.getDate() + "|" + device_info;

							logger.info(CLASS + " activateNewDevice device change mongo {} ", message);

							logModelServiceImpl.setLogModelDTO(true, device.getSubscriberUid(), null, "OTHER", null,
									message, startTime, endTime, null);

							if (subscriber2 != null) {
								subscriber2.setAppVersion(deviceInfo.getAppVersion());
								subscriber2.setOsVersion(deviceInfo.getOsVersion());
								subscriber2.setOsName(mobileOTPDto.getOsName());
								subscriber2.setDeviceInfo(mobileOTPDto.getDeviceInfo());
								subscriberRepoIface.save(subscriber2);
							}
							// update fcm token
							updateFcmToken(subscriber.getSubscriberUid(), mobileOTPDto.getFcmToken());

							return exceptionHandlerUtil.successResponse(
									"api.response.services.now.accessible.on.this.device.welcome.back");

						}
					}

				}
			}

			return exceptionHandlerUtil.createErrorResponse(
					"api.error.the.device.is.already.registered.with.either.same.or.different.email.id.and.mobile.number");

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + "updateSubscriberDeviceAndHistory Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);
		}

	}

	private void updateFcmToken(String suid, String fcmToken) {
		try {
			logger.info(CLASS + "updateFcmToken suid and fcmToken {}, {}", suid, fcmToken);
			SubscriberFcmToken subscriberFcmToken = fcmTokenRepoIface.findBysubscriberUid(suid);
			subscriberFcmToken.setFcmToken(fcmToken);
			subscriberFcmToken.setCreatedDate(AppUtil.getDate());
			fcmTokenRepoIface.save(subscriberFcmToken);
		} catch (JDBCConnectionException | ConstraintViolationException | DataException | LockAcquisitionException
				| PessimisticLockException | QueryTimeoutException | SQLGrammarException | GenericJDBCException ex) {
			ex.printStackTrace();
			logger.error(CLASS + "updateFcmToken Exception {}", ex.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			// return ExceptionHandlerUtil.handleException(e);
			logger.error(CLASS + "updateFcmToken Exception {}", e.getMessage());
		}

	}

	private NewDeviceDTO setNewDeviceResponse(Subscriber subscriber) {
		NewDeviceDTO newDeviceDTO = new NewDeviceDTO();
		try {
			newDeviceDTO.setNewDevice(true);
			if (subscriber == null)
				return newDeviceDTO;
			SubscriberDetailsReponseDTO responseDTO = new SubscriberDetailsReponseDTO();
			responseDTO.setSuID(subscriber.getSubscriberUid());
			newDeviceDTO.setEmail(subscriber.getEmailId());
			newDeviceDTO.setMobileNumber(subscriber.getMobileNumber());

			SubscriberStatus status = statusRepoIface.findBysubscriberUid(subscriber.getSubscriberUid());
			if (status != null)
				responseDTO.setSubscriberStatus(status.getSubscriberStatus());
			else
				responseDTO.setSubscriberStatus(null);
			List<SubscriberOnboardingData> onboardingDataList = onboardingDataRepoIface
					.getBySubUid(subscriber.getSubscriberUid());
			SubscriberOnboardingData onboardingData = null;
			SubscriberDetails subscriberDetails = new SubscriberDetails();

			if (onboardingDataList != null) {
				if (onboardingDataList.size() > 1) {
					onboardingData = findLatestOnboardedSub(onboardingDataList);

				} else {
					if (onboardingDataList.size() == 1)
						onboardingData = onboardingDataList.get(0);
					else
						onboardingData = null;
				}
			}
			if (onboardingData != null) {
				newDeviceDTO.setIdDocNumber(onboardingData.getIdDocNumber());
				
				if(selfieRequired) {
					ApiResponse response = subscriberServiceIface.getSubscriberSelfie(onboardingData.getSelfieUri());
					if (response.isSuccess()) {
						newDeviceDTO.setSelfieUri((String) response.getResult());
					} else {
						newDeviceDTO.setSelfieUri("");
					}
				}else {
					newDeviceDTO.setSelfieUri(onboardingData.getSelfie());
				}
				
				String method = onboardingData.getOnboardingMethod();
				ApiResponse editTemplateDTORes = templateServiceIface
						.getTemplateLatestById(onboardingData.getTemplateId());

				if (editTemplateDTORes.isSuccess()) {
					EditTemplateDTO editTemplateDTO = (EditTemplateDTO) editTemplateDTORes.getResult();
//					String certStatus = String.valueOf(subscriberCertificatesRepoIface.getSubscriberCertificateStatus(
//							subscriber.getSubscriberUid(), Constant.SIGN, Constant.ACTIVE));
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
									.findBysubscriberUid(subscriber.getSubscriberUid());
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
				newDeviceDTO.setSubscriberStatusDetails(responseDTO);
			} else {

				newDeviceDTO.setIdDocNumber(null);
				newDeviceDTO.setSelfieUri(null);
				newDeviceDTO.setSubscriberStatusDetails(responseDTO);
			}
			return newDeviceDTO;
		} catch (JDBCConnectionException | ConstraintViolationException | DataException | LockAcquisitionException
				| PessimisticLockException | QueryTimeoutException | SQLGrammarException | GenericJDBCException ex) {
			ex.printStackTrace();
			logger.error(CLASS + "setNewDeviceResponse Exception {}", ex.getMessage());
			return newDeviceDTO;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + "setNewDeviceResponse Exception {}", e.getMessage());
			return newDeviceDTO;
		}

	}

	@Override
	public ApiResponse validateSubscriberAndDevice(DeviceInfo deviceInfo, MobileOTPDto mobileOTPDto) {
		try {
			int countDevice = subscriberRepoIface.countSubscriberDevice(deviceInfo.getDeviceId());
			int countMobile = subscriberRepoIface.countSubscriberMobile(mobileOTPDto.getSubscriberMobileNumber());
			int countEmail = subscriberRepoIface
					.countSubscriberEmailId(mobileOTPDto.getSubscriberEmail().toLowerCase());

			logger.info(CLASS + " validateSubscriberAndDeviceNew ::: " + deviceInfo +"  mobileOTPDto :: "+mobileOTPDto);
			System.out.println("countDevice :: " + countDevice + " countMobile :: " + countMobile + " countEmail :: "
					+ countEmail);
			
			System.out.println("Device Id:: " + deviceInfo.getDeviceId() + " Mobile :: " + mobileOTPDto.getSubscriberMobileNumber() + " Email :: "
					+ mobileOTPDto.getSubscriberEmail().toLowerCase());

			Subscriber subscriber = subscriberRepoIface.getSubscriberDetailsByEmailAndMobile(
					mobileOTPDto.getSubscriberEmail(), mobileOTPDto.getSubscriberMobileNumber());

			NewDeviceDTO newDeviceDTO = setNewDeviceResponse(subscriber);

			String date = null;

			if (countEmail >= 1 && countMobile >= 1 && subscriber == null) {
				return exceptionHandlerUtil
						.createErrorResponse("api.error.this.mobile.no.is.already.register.with.different.email.id");
			} else if (countDevice == 0 && countEmail == 0 && countMobile == 0) {
				newDeviceDTO.setNewDevice(false);

				return exceptionHandlerUtil.createSuccessResponse("api.response.first.time.registering.onboarding",
						newDeviceDTO);

			} else if (countDevice == 0 && countEmail == 1 && countMobile == 0) {
				return exceptionHandlerUtil
						.createErrorResponse("api.error.this.email.id.is.already.used.with.differenet.mobile.no");
			} else if (countDevice == 0 && countEmail == 0 && countMobile == 1) {
				return exceptionHandlerUtil
						.createErrorResponse("api.error.this.mobile.number.is.already.used.with.differenet.email.id");

			} else if (countDevice == 0 && countEmail == 1 && countMobile == 1) {

				SubscriberDevice subdevice = deviceRepoIface.getSubscriber(subscriber.getSubscriberUid());

				if (subdevice.getDeviceStatus().equals(Constant.DEVICE_STATUS_ACTIVE)) {

					date = Objects.equals(subdevice.getCreatedDate(), subdevice.getUpdatedDate())
							? subdevice.getCreatedDate()
							: subdevice.getUpdatedDate();
					Optional<DevicePolicyModel> devicePolicyModel = Optional
							.ofNullable(devicePolicyRepository.getDevicePolicyHour());
					long devicePolicyHour = 0;
					if (devicePolicyModel.isPresent()) {
						devicePolicyHour = devicePolicyModel.get().getDevicePolicyHour();
						if (devicePolicyHour <= minhour) {
							devicePolicyHour = minhour;
						} else if (devicePolicyHour >= maxhour) {
							devicePolicyHour = maxhour;
						}
					} else {
						devicePolicyHour = minhour;
					}
					ApiResponse policyResponse = policyIface.checkPolicyRange(date, DeviceUpdatePolicy.PATTERN,
							devicePolicyHour);

					if (!policyResponse.isSuccess()) {

						return exceptionHandlerUtil.createFailedResponseWithCustomMessage(
								"Due to recent registration on another device, access here is on hold for now. You'll regain access to this device in just "
										+ devicePolicyHour + " hours. We appreciate your patience.",
								null);

					}
					return exceptionHandlerUtil.createSuccessResponse("api.response.new.device.is.ready.to.be.used",
							newDeviceDTO);

				}

				return exceptionHandlerUtil.createSuccessResponse("api.response.new.device.is.ready.to.be.used",
						newDeviceDTO);
			} else {
				SubscriberDevice subDevice = deviceRepoIface.findBydeviceUidAndStatus(deviceInfo.getDeviceId(),
						Constant.DEVICE_STATUS_ACTIVE);

				if (subDevice == null && subscriber == null) {
					newDeviceDTO.setNewDevice(false);
					return exceptionHandlerUtil.createSuccessResponse("api.response.first.time.registering.onboarding",
							newDeviceDTO);

				} else if (subDevice != null) {
					if (subDevice != null && subscriber == null) {

						return exceptionHandlerUtil.createErrorResponseWithResult(
								"api.error.this.mobile.number.is.already.used.with.differenet.email.id", newDeviceDTO);

					} else if (subDevice.getDeviceStatus().equalsIgnoreCase(Constant.DEVICE_STATUS_ACTIVE)
							&& !subDevice.getSubscriberUid().equals(subscriber.getSubscriberUid())) {

						return exceptionHandlerUtil.createErrorResponse(
								"api.error.this.mobile.number.is.already.used.with.differenet.email.id");

					} else {
						SubscriberDevice subscriberDevice = deviceRepoIface
								.getSubscriber(subscriber.getSubscriberUid());
						if (subscriber.getSubscriberUid().equals(subDevice.getSubscriberUid())) {
							newDeviceDTO.setNewDevice(false);

							return exceptionHandlerUtil.createSuccessResponse("api.response.app.is.re.installed",
									newDeviceDTO);
						}

						if (subscriberDevice.getDeviceStatus().equals(Constant.DEVICE_STATUS_ACTIVE)) {

							date = Objects.equals(subscriberDevice.getCreatedDate(), subscriberDevice.getUpdatedDate())
									? subscriberDevice.getCreatedDate()
									: subscriberDevice.getUpdatedDate();
							Optional<DevicePolicyModel> devicePolicyModel = Optional
									.ofNullable(devicePolicyRepository.getDevicePolicyHour());
							long devicePolicyHour = 0;
							if (devicePolicyModel.isPresent()) {
								devicePolicyHour = devicePolicyModel.get().getDevicePolicyHour();
								if (devicePolicyHour <= minhour) {
									devicePolicyHour = minhour;
								} else if (devicePolicyHour >= maxhour) {
									devicePolicyHour = maxhour;
								}
							} else {
								devicePolicyHour = minhour;
							}

							ApiResponse policyResponse = policyIface.checkPolicyRange(date, DeviceUpdatePolicy.PATTERN,
									devicePolicyHour);
							if (!policyResponse.isSuccess()) {

								return exceptionHandlerUtil.createFailedResponseWithCustomMessage(
										"Due to recent registration on another device, access here is on hold for now. You'll regain access to this device in just "
												+ devicePolicyHour + " hours. We appreciate your patience.",
										null);
							}

							return exceptionHandlerUtil
									.createSuccessResponse("api.response.new.device.is.ready.to.be.used", newDeviceDTO);

						} else {

							return exceptionHandlerUtil
									.createSuccessResponse("api.response.new.device.is.ready.to.be.used", newDeviceDTO);
						}
					}
				} else {
					SubscriberDevice subscriberDevice = deviceRepoIface.getSubscriber(subscriber.getSubscriberUid());

					System.out.println(" subscriberDevice   ::::::::::status "+subscriberDevice.getDeviceStatus()+" device id ::: "+subscriberDevice.getDeviceUid());
					if (subscriberDevice.getDeviceStatus().equalsIgnoreCase(Constant.DEVICE_STATUS_ACTIVE)) {

						System.out.println(" subscriberDevice   :::::inside if condition :::::status "+subscriberDevice.getDeviceStatus()+" device id ::: "+subscriberDevice.getDeviceUid());
						date = Objects.equals(subscriberDevice.getCreatedDate(), subscriberDevice.getUpdatedDate())
								? subscriberDevice.getCreatedDate()
								: subscriberDevice.getUpdatedDate();
						Optional<DevicePolicyModel> devicePolicyModel = Optional
								.ofNullable(devicePolicyRepository.getDevicePolicyHour());
						long devicePolicyHour = 0;
						if (devicePolicyModel.isPresent()) {
							devicePolicyHour = devicePolicyModel.get().getDevicePolicyHour();
							if (devicePolicyHour <= minhour) {
								devicePolicyHour = minhour;
							} else if (devicePolicyHour >= maxhour) {
								devicePolicyHour = maxhour;
							}
						} else {
							devicePolicyHour = minhour;
						}

						ApiResponse policyResponse = policyIface.checkPolicyRange(date, DeviceUpdatePolicy.PATTERN,
								devicePolicyHour);
						if (!policyResponse.isSuccess()) {

							return exceptionHandlerUtil.createFailedResponseWithCustomMessage(
									"Due to recent registration on another device, access here is on hold for now. You'll regain access to this device in just "
											+ devicePolicyHour + " hours. We appreciate your patience.",
									null);

						}

						return exceptionHandlerUtil.createSuccessResponse("api.response.new.device.is.ready.to.be.used",
								newDeviceDTO);
					} else {
						
						if(subscriberDevice.getDeviceUid().equalsIgnoreCase(deviceInfo.getDeviceId()) && subscriberDevice.getDeviceStatus().equalsIgnoreCase(Constant.DEVICE_STATUS_DISABLED) && subscriberDevice.getSubscriberUid().equalsIgnoreCase(subscriber.getSubscriberUid())) {
							newDeviceDTO.setNewDevice(false);
							return exceptionHandlerUtil.createSuccessResponse("api.response.new.device.is.ready.to.be.used",
									newDeviceDTO);
						}
						return exceptionHandlerUtil.createSuccessResponse("api.response.new.device.is.ready.to.be.used",
								newDeviceDTO);

					}

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + "validateSubscriberAndDevice Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	public void updateSubscriberDeviceHistory(SubscriberDevice oldDevice, String newDeviceUid) {
		// save to subscriber device history
		try {
			logger.error(CLASS + "updateSubscriberDeviceAndHistory oldDevice and newDeviceUid {}, {}", oldDevice,
					newDeviceUid);

			SubscriberDeviceHistory subscriberDeviceHistory = new SubscriberDeviceHistory();
			subscriberDeviceHistory.setDeviceUid(oldDevice.getDeviceUid());
			subscriberDeviceHistory.setDeviceStatus(Constant.DEVICE_STATUS_DISABLED);
			subscriberDeviceHistory.setSubscriberUid(oldDevice.getSubscriberUid());
			subscriberDeviceHistory.setCreatedDate(AppUtil.getDate());
			subscriberDeviceHistory.setUpdatedDate(AppUtil.getDate());
			subscriberDeviceHistoryRepoIface.save(subscriberDeviceHistory);

		} catch (JDBCConnectionException | ConstraintViolationException | DataException | LockAcquisitionException
				| PessimisticLockException | QueryTimeoutException | SQLGrammarException | GenericJDBCException ex) {
			ex.printStackTrace();
			logger.error(CLASS + "updateSubscriberDeviceAndHistory Exception {}", ex.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + "updateSubscriberDeviceAndHistory Exception {}", e.getMessage());
		}

	}
}
