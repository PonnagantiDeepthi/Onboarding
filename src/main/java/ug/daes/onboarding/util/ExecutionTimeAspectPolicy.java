package ug.daes.onboarding.util;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import jakarta.servlet.http.HttpServletRequest;

import jakarta.xml.bind.SchemaOutputResolver;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Component;

import ug.daes.onboarding.model.SubscriberDevice;
import ug.daes.onboarding.model.SubscriberDeviceHistory;
import ug.daes.onboarding.repository.SubscriberDeviceHistoryRepoIface;
import ug.daes.onboarding.repository.SubscriberDeviceRepoIface;

import static jakarta.xml.bind.DatatypeConverter.parseDate;

@Aspect
@Component
public class ExecutionTimeAspectPolicy {

	@Autowired
	MessageSource messageSource;

	@Autowired
	private SubscriberDeviceRepoIface subscriberDeviceRepoIface;

	@Autowired
	SubscriberDeviceHistoryRepoIface subscriberDeviceHistoryRepoIface;

	/** The Constant CLASS. */
	final static String CLASS = "ExecutionTimeAspect";

	@Pointcut("execution(* ug.daes.onboarding.controller.SubscriberController.saveSubscriberObData(..))")
	private void forsaveSubscriberObData() {
	};

	@Pointcut("execution(* ug.daes.onboarding.controller.SubscriberController.reOnboardAddSubscriberObData(..))")
	private void forreOnboardAddSubscriberObData() {
	};

	@Pointcut("execution(* ug.daes.onboarding.controller.SubscriberController.getSubscriberObData(..))")
	private void forgetSubscriberObData() {
	};

	@Pointcut("execution(* ug.daes.onboarding.controller.SubscriberController.resetPin(..))")
	private void forresetPin() {
	};

//    @Pointcut("execution(* ug.daes.onboarding.controller.TemplateController.getActviteTemplate(..))")
//    private void forgetActviteTemplate(){};

//	@Pointcut("execution(* ug.daes.onboarding.controller.LogController.saveNiraApiLogs(..))")
//	private void forLogController() {
//	};

	@Pointcut("execution(* ug.daes.onboarding.controller.UpdateSubscriberController.updatePhoneNumber(..))")
	private void forUpdateSubscriberController() {
	};

	@Around("forsaveSubscriberObData() || forreOnboardAddSubscriberObData() || forgetSubscriberObData() || forresetPin() || forUpdateSubscriberController()") // methods
	public Object controllerPolicy(ProceedingJoinPoint joinPoint) throws Throwable {
		return checkPolicy(joinPoint);
	}

	private Object checkPolicy(ProceedingJoinPoint joinPoint) throws Throwable {
		String methodName = joinPoint.getSignature().toShortString();

		// System.out.println("method name: " + methodName);
		String deviceUid = "";
		String appVersion = "";
		for (Object arg : joinPoint.getArgs()) {
			if (arg instanceof HttpServletRequest) {
				HttpServletRequest httpServletRequest = (HttpServletRequest) arg;

				deviceUid = httpServletRequest.getHeader("deviceId");
				appVersion = httpServletRequest.getHeader("appVersion");

				break;
			}
		}

//		Optional<SubscriberDeviceHistory> subscriberDeviceHistoryOptional = Optional
//				.ofNullable((SubscriberDeviceHistory) subscriberDeviceHistoryRepoIface.findBydeviceUid(deviceUid));
//

		List<SubscriberDeviceHistory> historyList = subscriberDeviceHistoryRepoIface.findBydeviceUid(deviceUid);

		System.out.println(" historyList :: "+historyList);

		SubscriberDeviceHistory latestHistory = historyList.isEmpty() ? null : historyList.get(0);

		//SubscriberDeviceHistory latestHistory =  getLatest(historyList);

		Optional<SubscriberDeviceHistory> subscriberDeviceHistoryOptional = Optional.ofNullable(latestHistory);


		System.out.println(" subscriberDeviceHistoryOptional :::"+subscriberDeviceHistoryOptional);

		SubscriberDevice checkSubscriberDetails = null;
		//List<SubscriberDevice> subscriberDeviceDetails =
		//subscriberDeviceRepoIface.findBydeviceUid(deviceUid);

		String deviceId = deviceUid;

		List<SubscriberDevice> subscriberDeviceDetails =  subscriberDeviceRepoIface.findBydeviceUid(deviceId);
		System.out.println(" subscriberDeviceDetails :: "+subscriberDeviceDetails);

		SubscriberDevice subscriberDevices =  getLatest(subscriberDeviceDetails);
		System.out.println(" subscriberDevices  11 :::"+subscriberDevices);

		Object result;
		boolean checkPolicy = true;
		boolean deviceEmpty = false;
		if (deviceUid.equals("WEB")) {
			checkPolicy = true;
		} else {
			if (appVersion == null || appVersion.equals("") || appVersion == "") {
				System.out.println("appVersion is empty appVersion and deviceUid " + appVersion + "-- " + deviceUid);
				deviceEmpty = true;

			} else if (subscriberDeviceHistoryOptional.isPresent()) {

				System.out.println(" hello ");

				// Optional<SubscriberDevice> subscriberDevice =
				// Optional.ofNullable(subscriberDeviceRepoIface.findBydeviceUidAndStatus(deviceUid,"ACTIVE"));
				checkSubscriberDetails = subscriberDeviceRepoIface
						.getSubscriber(subscriberDeviceHistoryOptional.get().getSubscriberUid());
				SubscriberDevice subscriberDevice = subscriberDeviceRepoIface.findBydeviceUidAndStatus(deviceUid,
						"ACTIVE");
				if (subscriberDevice == null) {
					System.out.println(" hello 111 ");
					checkPolicy = false;

				} else if (subscriberDevice.getDeviceStatus() == "DISABLED"
						|| subscriberDevice.getDeviceStatus().equalsIgnoreCase("DISABLED")) {

					System.out.println(" hello 22222222 ");
					checkPolicy = false;
					System.out.println("inside else if");
					result = AppUtil.createApiResponse(false,
							messageSource.getMessage(
									"api.error.account.registered.on.new.device.services.disabled.on.this.device", null,
									Locale.ENGLISH),
							null);
				} else {

					System.out.println(" hello 765678 ");
					checkPolicy = true;
				}

			} else if (subscriberDevices == null) {
				System.out.println(" hii ");
				checkPolicy = false;

			} else if (subscriberDevices.getDeviceStatus() == "ACTIVE"
					|| subscriberDevices.getDeviceStatus().equalsIgnoreCase("ACTIVE")) {
				System.out.println(" jkl  ;lkjhgf");
				checkPolicy = true;
				System.out.println("inside else if active");

			} else if (subscriberDevices.getDeviceStatus() == "DISABLED"
					|| subscriberDevices.getDeviceStatus().equalsIgnoreCase("DISABLED")) {
				System.out.println(" ldkjfhdfjh ");
				checkPolicy = false;
				System.out.println("inside else if DISABLED");

			} else {

				checkPolicy = false;
				System.out.println("inside else");
				result = AppUtil.createApiResponse(false,
						messageSource.getMessage(
								"api.error.account.registered.on.new.device.services.disabled.on.this.device", null,
								Locale.ENGLISH),
						null);
			}
		}

		if (deviceEmpty) {
			result = AppUtil.createApiResponse(false,
					messageSource.getMessage("api.error.please.update.your.app", null, Locale.ENGLISH), null);
		} else if (checkPolicy) {
			result = joinPoint.proceed();
		} else {

			// Account registered on new device, services temporarily disabled on this
			// device.
			if (subscriberDevices  == null && checkSubscriberDetails == null) {
				result = AppUtil.createApiResponse(false,
						messageSource.getMessage("api.error.subscriber.not.found", null, Locale.ENGLISH), null);
			} else {
				result = AppUtil.createApiResponse(false,
						messageSource.getMessage(
								"api.error.account.registered.on.new.device.services.disabled.on.this.device", null,
								Locale.ENGLISH),
						null);
			}

//			result = AppUtil.createApiResponse(false,"We apologize for any inconvenience.  You can use the service after " +remainHour+ " hours, as it seems you changed your Device.",null);
		}

		return result;
	}

	public SubscriberDevice getLatest(List<SubscriberDevice> list) {

		//List<SubscriberDevice> list = deviceRepoIface.findByDeviceUid(deviceUid);

		return list.stream()
				.sorted(Comparator.comparing(sd -> parseDate(sd.getUpdatedDate()), Comparator.reverseOrder()))
				.findFirst()
				.orElse(null);
	}

	private LocalDateTime parseDate(String date) {
		if (date.contains("T")) {
			return LocalDateTime.parse(date); // ISO format
		} else {
			return LocalDateTime.parse(date.replace(" ", "T"));
		}
	}

}
