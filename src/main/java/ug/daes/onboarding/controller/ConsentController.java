package ug.daes.onboarding.controller;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.*;

import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.exceptions.ExceptionHandlerUtil;
import ug.daes.onboarding.model.Consent;
import ug.daes.onboarding.model.ConsentHistory;
import ug.daes.onboarding.repository.ConsentHistoryRepo;
import ug.daes.onboarding.repository.ConsentRepoIface;
import ug.daes.onboarding.service.iface.ConsentIface;
import ug.daes.onboarding.service.impl.KafkaSender;
//import ug.daes.onboarding.service.impl.RabbitMQSender;
import ug.daes.onboarding.util.AppUtil;

@RestController
@CrossOrigin
public class ConsentController {

	private static Logger logger = LoggerFactory.getLogger(ConsentController.class);

	/** The Constant CLASS. */
	final static String CLASS = "ConsentController";

	@Autowired
	MessageSource messageSource;

	@Autowired
	private ConsentRepoIface consentRepoIface;

	@Autowired
	private ConsentHistoryRepo consentHistoryRepo;

	@Autowired
	ConsentIface consentIface;
    @Autowired
	KafkaSender msender;
	@Autowired
	ExceptionHandlerUtil exceptionHandlerUtil;

	@GetMapping(value = "/api/activte/consent")
	public ApiResponse getActivteConsent() {
		Consent activeConsent = new Consent();
		try {
			activeConsent = consentRepoIface.getActiveConsent();


			if (activeConsent != null) {
				return exceptionHandlerUtil.createSuccessResponse("api.response.consent", activeConsent);
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.no.active.consent.found");
			}
		} catch (Exception e) {
			logger.error(CLASS + "Get Active Consent Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@GetMapping(value = "/api/get/list/consent")
	public ApiResponse getConsentList() {
		List<Consent> consent = new ArrayList<Consent>();
		try {
			consent = consentRepoIface.findAll();
			if (consent != null) {

				return exceptionHandlerUtil.createSuccessResponse("api.response.consent", consent);
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.empty");

			}
		} catch (Exception e) {
			logger.error(CLASS + "Get Consent List {} ", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@GetMapping(value = "/api/get/consent/id")
	public ApiResponse getConsentById(@RequestParam int id) {

		ConsentHistory consent = new ConsentHistory();
		try {
			consent = consentHistoryRepo.findTopByConsentIdOrderByCreatedOnDesc(id);
			if (consent != null) {

				return exceptionHandlerUtil.createSuccessResponse("api.response.consent", consent);

			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.empty");

			}
		} catch (Exception e) {
			logger.error(CLASS + "Get Consent By-id {} ", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);

		}

	}

	@PostMapping(value = "/api/add/consent")
	public ApiResponse addConsent(@RequestBody Consent consent) {
		logger.info(CLASS + "Add Consent :: {}", consent);
		Consent savedConsent = new Consent();
		try {
			if (consent.getConsent() == null || consent.getConsent().equals("")) {
				return AppUtil.createApiResponse(false, "api.error.consent.is.empty", null);
			}
			consent.setCreatedOn(AppUtil.getDate());
			consent.setUpdatedOn(AppUtil.getDate());
			consent.setStatus("INACTIVE");
			savedConsent = consentRepoIface.save(consent);
			if (savedConsent != null) {
				return exceptionHandlerUtil.successResponse("api.response.consent.saved");

			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.consent.not.usaved");

			}
		} catch (Exception e) {
			logger.error(CLASS + "Add Consent Exception :: " + e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	@GetMapping(value = "/api/update/consent/status")
	public ApiResponse updateConsentStatus(@RequestParam int consentId, @RequestParam String status) {
		logger.info(CLASS + "Update Consent Status :: consentId and status {},{}", consentId, status);
		Consent savedConsent = new Consent();
		try {
			if (status.equals("Active") || status.equals("ACTIVE")) {
				consentRepoIface.updateConsentStatusActive(consentId, status);
			} else {
				consentRepoIface.updateConsentStatusInactive(consentId, status);
			}
			if (savedConsent != null) {
				return exceptionHandlerUtil.successResponse("api.response.consent.updated");

			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.consent.not.updated");

			}
		} catch (Exception e) {
			logger.error(CLASS + "Update Consent Status Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	@PostMapping("/sign-data/for/consent")
	public ApiResponse signData(@RequestHeader HttpHeaders httpHeaders) {
		return consentIface.signData(httpHeaders);
	}

}
