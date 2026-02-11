package ug.daes.onboarding.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.stereotype.Service;

import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.constant.Constant;
import ug.daes.onboarding.dto.EditTemplateDTO;
import ug.daes.onboarding.dto.MobileTemplateDTO;
import ug.daes.onboarding.dto.SubscriberDTO;
import ug.daes.onboarding.dto.TemplateApproveDTO;
import ug.daes.onboarding.dto.TemplateDTO;
import ug.daes.onboarding.exceptions.ExceptionHandlerUtil;
import ug.daes.onboarding.model.MapMethodOnboardingStep;
import ug.daes.onboarding.model.OnboardingMethod;
import ug.daes.onboarding.model.OnboardingSteps;
import ug.daes.onboarding.model.SubscriberOnboardingTemplate;
import ug.daes.onboarding.repository.MapMethodObStepRepoIface;
import ug.daes.onboarding.repository.OnBoardingMethodRepoIface;
import ug.daes.onboarding.repository.OnBoardingStepRepoIface;
import ug.daes.onboarding.repository.OnBoardingTemplateRepoIface;
import ug.daes.onboarding.service.iface.TemplateServiceIface;
import ug.daes.onboarding.util.AppUtil;

@Service
public class OnBoardingTemplateServiceImpl implements TemplateServiceIface {

	private static Logger logger = LoggerFactory.getLogger(OnBoardingTemplateServiceImpl.class);

	/** The Constant CLASS. */
	final static String CLASS = "OnBoardingTemplateServiceImpl";

	@Autowired
	OnBoardingMethodRepoIface methodRepoIface;

	@Autowired
	OnBoardingTemplateRepoIface templateRepoIface;

	@Autowired
	OnBoardingStepRepoIface stepRepoIface;

	@Autowired
	MapMethodObStepRepoIface mapStepRepoIface;

	@Autowired
	MessageSource messageSource;

	@Autowired
	ExceptionHandlerUtil exceptionHandlerUtil;

	@Autowired
	private OnBoardingTemplateRepoIface templateRepo;

	@Override
	public ApiResponse getTemplates() {
		List<SubscriberOnboardingTemplate> templates = new ArrayList<SubscriberOnboardingTemplate>();

		try {
			templates = templateRepoIface.getAllTemplate();

			if (templates != null) {
				logger.info(CLASS + " getTemplates res {}", templates);
				return exceptionHandlerUtil.createSuccessResponse("api.response.template.list", templates);
			} else {
				return exceptionHandlerUtil.successResponse("api.response.template.list.is.empty");
			}
		} catch (Exception e) {
			logger.error(CLASS + " getTemplates Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@SuppressWarnings("unused")
	@Override
	public ApiResponse getActiveTemplate(SubscriberDTO subscriberDTO) {
		logger.info(CLASS + " SubscriberDTO received: {}", subscriberDTO);
		logger.info(CLASS + " getActiveTemplate req {}", subscriberDTO.getMethodName());
		SubscriberOnboardingTemplate template = new SubscriberOnboardingTemplate();
		EditTemplateDTO templateDTO = new EditTemplateDTO();
		try {
			if (subscriberDTO.getMethodName() != null) {
				template = templateRepoIface.getPublishTemplate(subscriberDTO.getMethodName(), "PUBLISHED");

				List<MapMethodOnboardingStep> stepList = mapStepRepoIface.findBytemplateId(template.getTemplateId());

				HashMap<String, MapMethodOnboardingStep> hm = new HashMap<String, MapMethodOnboardingStep>();
				stepList.forEach(mapMethodOnboardingStep -> {
					hm.put(mapMethodOnboardingStep.getOnboardingStep(), mapMethodOnboardingStep);
				});
				templateDTO.setSteps(hm);
				templateDTO.setTemplateName(template.getTemplateName());
				templateDTO.setTemplateMethod(template.getTemplateMethod());
				templateDTO.setPublishedStatus(template.getPublishedStatus());
				templateDTO.setState(template.getState());
				templateDTO.setTemplateId(template.getTemplateId());

				if (templateDTO != null) {
					logger.info(CLASS + " getActviteTemplate res Template {}", templateDTO);
					return exceptionHandlerUtil.createSuccessResponse("api.response.template", templateDTO);
				} else {
					return exceptionHandlerUtil.createErrorResponse("api.error.template.not.found");
				}
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.method.name.is.empty");
			}
		} catch (Exception e) {
			logger.error(CLASS + " getActviteTemplate Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}

	}
//@SuppressWarnings("unused")
//@Override
//public ApiResponse getActiveTemplate(SubscriberDTO subscriberDTO) {
//	logger.info(CLASS + " SubscriberDTO received: {}", subscriberDTO);
//
//	try {
//		if (subscriberDTO.getMethodName() != null && !subscriberDTO.getMethodName().trim().isEmpty()) {
//			String methodName = subscriberDTO.getMethodName().trim();
//
//			// 🔍 DEBUG LOG
//			logger.info("Method name from request: '{}'", methodName);
//
//			// ✅ FIX: Convert to UPPERCASE to match database
//			String normalizedMethodName = methodName.toUpperCase();
//			logger.info("Normalized method name: '{}'", normalizedMethodName);
//
//			String publishedStatus = "PUBLISHED";
//
//			// Query with UPPERCASE method name
//			SubscriberOnboardingTemplate template = templateRepoIface.getPublishTemplate(normalizedMethodName, publishedStatus);
//
//			if (template == null) {
//				logger.error("❌ NO TEMPLATE FOUND for: '{}' (normalized: '{}')", methodName, normalizedMethodName);
//				return exceptionHandlerUtil.createErrorResponse("api.error.template.not.found");
//			}
//
//			// SUCCESS - Continue with your existing logic
//			List<MapMethodOnboardingStep> stepList = mapStepRepoIface.findBytemplateId(template.getTemplateId());
//
//			HashMap<String, MapMethodOnboardingStep> hm = new HashMap<String, MapMethodOnboardingStep>();
//			stepList.forEach(mapMethodOnboardingStep -> {
//				hm.put(mapMethodOnboardingStep.getOnboardingStep(), mapMethodOnboardingStep);
//			});
//
//			EditTemplateDTO templateDTO = new EditTemplateDTO();
//			templateDTO.setSteps(hm);
//			templateDTO.setTemplateName(template.getTemplateName());
//			templateDTO.setTemplateMethod(template.getTemplateMethod());
//			templateDTO.setPublishedStatus(template.getPublishedStatus());
//			templateDTO.setState(template.getState());
//			templateDTO.setTemplateId(template.getTemplateId());
//
//			logger.info(CLASS + " ✅ SUCCESS - Template found: {}", templateDTO.getTemplateName());
//			return exceptionHandlerUtil.createSuccessResponse("api.response.template", templateDTO);
//
//		} else {
//			return exceptionHandlerUtil.createErrorResponse("api.error.method.name.is.empty");
//		}
//	} catch (Exception e) {
//		logger.error(CLASS + " getActiveTemplate Exception {}", e.getMessage());
//		e.printStackTrace();
//		return ExceptionHandlerUtil.handleException(e);
//	}
//}

//	@SuppressWarnings("unused")
//	@Override
//	public ApiResponse getActiveTemplate(SubscriberDTO subscriberDTO) {
//		logger.info(CLASS + " SubscriberDTO received: {}", subscriberDTO);
//
//		try {
//			if (subscriberDTO.getMethodName() != null && !subscriberDTO.getMethodName().trim().isEmpty()) {
//				String methodName = subscriberDTO.getMethodName().trim();
//
//				// 🔍 CRITICAL DEBUG - SEE EXACT METHOD NAME
//				logger.info("=== DEBUG METHOD NAME ===");
//				logger.info("Method name from request: '{}'", methodName);
//				logger.info("Method name length: {}", methodName.length());
//				logger.info("Available in DB: UNID, NIN, PASSPORT, E-PASSPORT");
//				logger.info("========================");
//
//				String publishedStatus = "PUBLISHED";
//				SubscriberOnboardingTemplate template = templateRepoIface.getPublishTemplate(methodName, publishedStatus);
//
//				if (template == null) {
//					logger.error("❌ NO TEMPLATE FOUND for: '{}'", methodName);
//					logger.error("💡 Check if it matches exactly: UNID, NIN, PASSPORT, E-PASSPORT");
//					return exceptionHandlerUtil.createErrorResponse("api.error.template.not.found");
//				}
//
//				// If we get here, template was found - continue with your logic
//				List<MapMethodOnboardingStep> stepList = mapStepRepoIface.findBytemplateId(template.getTemplateId());
//
//				HashMap<String, MapMethodOnboardingStep> hm = new HashMap<String, MapMethodOnboardingStep>();
//				stepList.forEach(mapMethodOnboardingStep -> {
//					hm.put(mapMethodOnboardingStep.getOnboardingStep(), mapMethodOnboardingStep);
//				});
//
//				EditTemplateDTO templateDTO = new EditTemplateDTO();
//				templateDTO.setSteps(hm);
//				templateDTO.setTemplateName(template.getTemplateName());
//				templateDTO.setTemplateMethod(template.getTemplateMethod());
//				templateDTO.setPublishedStatus(template.getPublishedStatus());
//				templateDTO.setState(template.getState());
//				templateDTO.setTemplateId(template.getTemplateId());
//
//				logger.info(CLASS + " ✅ SUCCESS - Template found: {}", templateDTO.getTemplateName());
//				return exceptionHandlerUtil.createSuccessResponse("api.response.template", templateDTO);
//
//			} else {
//				return exceptionHandlerUtil.createErrorResponse("api.error.method.name.is.empty");
//			}
//		} catch (Exception e) {
//			logger.error(CLASS + " getActiveTemplate Exception {}", e.getMessage());
//			e.printStackTrace();
//			return ExceptionHandlerUtil.handleException(e);
//		}
//	}
	@Override
	public ApiResponse saveTemplates(TemplateDTO templateDTO) {
		logger.info(CLASS + " saveTemplates req {}" + templateDTO.getTemplateName());
		SubscriberOnboardingTemplate template = new SubscriberOnboardingTemplate();

		List<MapMethodOnboardingStep> onboardingStepList = new ArrayList<MapMethodOnboardingStep>();

		List<MapMethodOnboardingStep> onboardingStepSavedList = new ArrayList<MapMethodOnboardingStep>();

		try {
			int count = templateRepo.isTemplateExistWithMethod(templateDTO.getTemplateName(), templateDTO.getTemplateMethod());
			if (count > 0)
			{
				return exceptionHandlerUtil.createErrorResponse("api.error.template.methodname.alreday.exist");
			}
			else {
				if (templateDTO != null) {
					template.setTemplateName(templateDTO.getTemplateName());
					template.setTemplateMethod(templateDTO.getTemplateMethod());
					template.setCreatedDate(AppUtil.getDate());
					template.setUpatedDate(AppUtil.getDate());
					template.setApprovedBy(templateDTO.getApprovedBy());
					if (templateDTO.getTemplateId() == 0) {
						template.setCreatedBy(templateDTO.getCreatedBy());
					} else {
						SubscriberOnboardingTemplate templateStatus = templateRepoIface
								.findBytemplateId(templateDTO.getTemplateId());
						template.setTemplateId(templateDTO.getTemplateId());
						if (templateStatus != null) {
							if (templateStatus.getPublishedStatus() == "PUBLISHED"
									|| templateStatus.getPublishedStatus().equals("PUBLISHED")) {
								return exceptionHandlerUtil.successResponse(
										"api.response.your.template.status.is.published.please.unpublished.it.before.making.any.modifications");
							} else {
								template.setUpdatedBy(templateDTO.getUpdatedBy());
								for (@SuppressWarnings("unused")
								OnboardingSteps steps : templateDTO.getSteps()) {
									mapStepRepoIface.deleteBytemplateId(templateDTO.getTemplateId());
								}
							}
						} else {
							template.setUpdatedBy(templateDTO.getUpdatedBy());
							for (@SuppressWarnings("unused")
							OnboardingSteps steps : templateDTO.getSteps()) {
								mapStepRepoIface.deleteBytemplateId(templateDTO.getTemplateId());
							}
						}
					}
					template.setPublishedStatus("UNPUBLISHED");
					template = templateRepoIface.save(template);

					int i = 1;
					for (OnboardingSteps steps : templateDTO.getSteps()) {
						MapMethodOnboardingStep mapOnboardingStep = new MapMethodOnboardingStep();
						mapOnboardingStep.setCreatedDate(AppUtil.getDate());
						mapOnboardingStep.setIntegrationUrl(steps.getIntegrationUrl());
						mapOnboardingStep.setMethodName(templateDTO.getTemplateMethod());
						mapOnboardingStep.setOnboardingStep(steps.getOnboardingStep());
						mapOnboardingStep.setOnboardingStepThreshold(steps.getOnboardingStepThreshold());
						mapOnboardingStep.setAndriodTFliteThreshold(steps.getAndriodTFliteThreshold());
						mapOnboardingStep.setAndriodDTTThreshold(steps.getAndriodDTTThreshold());
						mapOnboardingStep.setIosTFliteThreshold(steps.getIosTFliteThreshold());
						mapOnboardingStep.setIosDTTThreshold(steps.getIosDTTThreshold());
						mapOnboardingStep.setTemplateId(template.getTemplateId());
						mapOnboardingStep.setSequence(i);
						onboardingStepList.add(mapOnboardingStep);
						i++;
					}

					onboardingStepSavedList = mapStepRepoIface.saveAll(onboardingStepList);

					if (template != null && onboardingStepSavedList != null) {
						logger.info(CLASS + " saveTemplates  res  Template Saved  {}", template);
						return exceptionHandlerUtil.createSuccessResponse("api.response.template.saved", template);

					} else {
						return exceptionHandlerUtil.createErrorResponse("api.error.template.not.saved");
					}
				} else {
					return exceptionHandlerUtil.createErrorResponse("api.error.saving.template.entity.cant.be.null");
				}
			}
		} catch (Exception e) {
			logger.error(CLASS + " saveTemplates Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@SuppressWarnings("unused")
	@Override
	public ApiResponse getTemplateById(int id) {
		logger.info(CLASS + " getTemplateById req  id {}", id);
		SubscriberOnboardingTemplate template = new SubscriberOnboardingTemplate();
		MobileTemplateDTO templateDTO = new MobileTemplateDTO();
		try {
			template = templateRepoIface.findBytemplateId(id);

			List<MapMethodOnboardingStep> stepList = mapStepRepoIface.findBytemplateId(template.getTemplateId());

			for (MapMethodOnboardingStep mapMethodOnboardingStep : stepList) {

				if (mapMethodOnboardingStep.getOnboardingStep().equals("SELFIE_CAPTURING")) {
					mapMethodOnboardingStep.setOnboardingStepId(1);
				} else if (mapMethodOnboardingStep.getOnboardingStep().equals("MRZ_SCANNING")) {
					mapMethodOnboardingStep.setOnboardingStepId(2);
				} else if (mapMethodOnboardingStep.getOnboardingStep().equals("PDF417_READING")) {
					mapMethodOnboardingStep.setOnboardingStepId(3);
				} else if (mapMethodOnboardingStep.getOnboardingStep().equals("NFC")) {
					mapMethodOnboardingStep.setOnboardingStepId(4);
				} else if (mapMethodOnboardingStep.getOnboardingStep().equals("UNID")) {
					mapMethodOnboardingStep.setOnboardingStepId(5);
				}
			}
			logger.info(CLASS + " getTemplateById req stepList {}", stepList);
			templateDTO.setSteps(stepList);
			templateDTO.setTemplateName(template.getTemplateName());
			templateDTO.setTemplateMethod(template.getTemplateMethod());
			templateDTO.setPublishedStatus(template.getPublishedStatus());
			templateDTO.setState(template.getState());
			templateDTO.setTemplateId(template.getTemplateId());

			if (template != null) {
				logger.info(CLASS + " getTemplateById res  Template by Id {}", templateDTO);
				return exceptionHandlerUtil.createSuccessResponse("api.response.template.by.id", templateDTO);
			} else {
				return exceptionHandlerUtil.successResponse("api.error.template.not.found");
			}
		} catch (Exception e) {
			logger.error(CLASS + "getTemplateById  Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Override
	public ApiResponse getMethods() {

		List<OnboardingMethod> methods = new ArrayList<OnboardingMethod>();

		try {
			methods = methodRepoIface.findAll();
			if (methods == null) {
				return exceptionHandlerUtil.successResponse("api.response.method.list.is.empty");
			} else {
				logger.info(CLASS + " getMethod res Method List {}", methods);
				return exceptionHandlerUtil.createSuccessResponse("api.response.method.list", methods);
			}
		} catch (Exception e) {
			logger.error(CLASS + " getMethod Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Override
	public ApiResponse getOnBoardingSteps() {
		List<OnboardingSteps> steps = new ArrayList<OnboardingSteps>();

		try {
			steps = stepRepoIface.findAll();

			if (steps != null) {
				logger.info(CLASS + " getOnBoardingStep res List of Steps {}", steps);
				return exceptionHandlerUtil.createSuccessResponse("api.response.list.of.steps", steps);
			} else {
				return exceptionHandlerUtil.successResponse("api.response.list.of.steps");

			}
		} catch (Exception e) {
			logger.error(CLASS + " getOnBoardingSteps Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}

	}

	@Override
	public ApiResponse updateTemplateStatus(int id, String status) {
		logger.info(CLASS + " updateTemplateStatus req id  {} and  status {} ", id, status);
		SubscriberOnboardingTemplate template = templateRepoIface.findBytemplateId(id);
		try {

			if (template.getPublishedStatus() == status || template.getPublishedStatus().equals(status)) {
				return exceptionHandlerUtil.createErrorResponse("api.response.template.is.already" + " " + status);
			}
			template.setState(Constant.ACTIVE);
			template.setPublishedStatus(status);
			template = templateRepoIface.save(template);
			if (template != null) {
				logger.info(CLASS + " updateTemplateStatus res Template has been {},  {} ", status, template);
				return exceptionHandlerUtil.createSuccessResponse("api.response.template.has.been" + " " + status,
						template);
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.template.status.not.updated");
			}
		} catch (Exception e) {
			logger.error(CLASS + " updateTemplateStatus Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}

	}

	@Override
	public ApiResponse testTemplate(SubscriberDTO subscriberDTO) {
		logger.info(CLASS + " testActviteTemplate req  {}", subscriberDTO);
		SubscriberOnboardingTemplate template = new SubscriberOnboardingTemplate();
		EditTemplateDTO templateDTO = new EditTemplateDTO();
		try {
			if (subscriberDTO.getMethodName() != null) {
				template = templateRepoIface.getPublishTemplate(subscriberDTO.getMethodName(), "PUBLISHED");

				List<MapMethodOnboardingStep> stepList = mapStepRepoIface.findBytemplateId(template.getTemplateId());

//				convert on-boarding steps to HashMap
				HashMap<String, MapMethodOnboardingStep> hm = new HashMap<String, MapMethodOnboardingStep>();
				stepList.forEach(mapMethodOnboardingStep -> {
					hm.put(mapMethodOnboardingStep.getOnboardingStep(), mapMethodOnboardingStep);
				});
				templateDTO.setSteps(hm);

//				templateDTO.setSteps(stepList);
				templateDTO.setTemplateName(template.getTemplateName());
				templateDTO.setTemplateMethod(template.getTemplateMethod());
				templateDTO.setPublishedStatus(template.getPublishedStatus());
				templateDTO.setState(template.getState());
				templateDTO.setTemplateId(template.getTemplateId());

				if (templateDTO != null) {
					logger.info(CLASS + " testActviteTemplate res Template {}", templateDTO);
					return exceptionHandlerUtil.createSuccessResponse("api.response.template", templateDTO);

				} else {
					return exceptionHandlerUtil.successResponse("api.response.template.not.found");

				}
			} else {
				return exceptionHandlerUtil.successResponse("api.response.method.name.is.empty");

			}
		} catch (Exception e) {
			logger.error(CLASS + " testActviteTemplate Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	@Override
	public ApiResponse templateApprove(TemplateApproveDTO templateApproveDTO) {
		logger.info(CLASS + " approveTemplate  req  {}", templateApproveDTO);
		try {
			SubscriberOnboardingTemplate template = templateRepoIface
					.findBytemplateId(templateApproveDTO.getTemplateId());

			if (template.getPublishedStatus() == "PUBLISHED" || template.getPublishedStatus().equals("PUBLISHED")) {
				return exceptionHandlerUtil.successResponse(
						"api.response.your.template.status.is.published.please.unpublished.it.before.making.any.modifications");

			} else {
				if (templateApproveDTO.isApprove()) {
//					template.setState(templateApproveEnum.ACTIVE.toString());
				} else {
//					template.setState(templateApproveEnum.DECLINED.toString());
				}
				template.setRemarks(templateApproveDTO.getRemarks());
			}

			template = templateRepoIface.save(template);
			if (template != null) {
				logger.info(CLASS + " approveTemplate res Template State Updated {}", template);
				return exceptionHandlerUtil.createSuccessResponse("api.response.template.state.updated", template);

			} else {
				return exceptionHandlerUtil.successResponse("api.response.template.state.not.updated");

			}
		} catch (Exception e) {
			logger.error(CLASS + " approveTemplate Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);

		}

	}

	enum templateApproveEnum {
		NEW, ACTIVE, MODIFIED, DECLINED, DELETE, DELETED
	}

	@Override
	public ApiResponse deleteTemplateById(int id) {
		logger.info(CLASS + " deleteTemplateById req id {}", id);
		try {
			SubscriberOnboardingTemplate template = templateRepoIface.findBytemplateId(id);
			if (template == null) {
				return exceptionHandlerUtil.createErrorResponse("api.error.template.not.found");

			}
			if (template.getPublishedStatus().equals("PUBLISHED")) {
				return exceptionHandlerUtil.createErrorResponse("api.error.template.is.in.use.cannot.deleted");

			} else if (template.getPublishedStatus().equals("UNPUBLISHED")
					|| template.getPublishedStatus() == "UNPUBLISHED") {
				template.setPublishedStatus(templateApproveEnum.DELETED.toString());
				template.setState(templateApproveEnum.MODIFIED.toString());
				template.setUpatedDate(AppUtil.getDate());
				templateRepoIface.save(template);
				logger.info(CLASS + " deleteTemplateById  res  Template Status to DELETED ");
				return exceptionHandlerUtil.successResponse("api.response.template.status.to.deleted");

			} else if (template.getPublishedStatus().equals("DELETED") || template.getPublishedStatus() == "DELETED") {
				return exceptionHandlerUtil.createErrorResponse("api.error.template.already.deleted");

			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.unpublished.the.template.first");

			}
		} catch (Exception e) {
			logger.error(CLASS + " deleteTemplateById  Exception  {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	@Override
	public ApiResponse isTemplateAlreadyExixts(String templateName, String methodId) {
		logger.info(CLASS + " isTemplateExist req   templateName {}  and MethodName {} ", templateName, methodId);
		try {
			int a = templateRepoIface.isTemplateExist(templateName);
			if (a == 0) {
				return exceptionHandlerUtil.createErrorResponse("api.error.not.exist");

			} else {
				return exceptionHandlerUtil.successResponse("api.response.exist");

			}
		} catch (Exception e) {
			logger.error(CLASS + " isTemplateAlreadyExixts Exception {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);

		}
	}

	@Override
	public ApiResponse getTemplateLatestById(int id) {
		logger.info(CLASS + " getTemplateLatestById req id  {}", id);
		SubscriberOnboardingTemplate template = new SubscriberOnboardingTemplate();
		EditTemplateDTO templateDTO = new EditTemplateDTO();
		template = templateRepoIface.findBytemplateId(id);

		try {
			if (template != null) {
				List<MapMethodOnboardingStep> stepList = mapStepRepoIface.findBytemplateId(template.getTemplateId());

				HashMap<String, MapMethodOnboardingStep> hm = new HashMap<String, MapMethodOnboardingStep>();
				stepList.forEach(mapMethodOnboardingStep -> {
					hm.put(mapMethodOnboardingStep.getOnboardingStep(), mapMethodOnboardingStep);
				});
				templateDTO.setTemplateId(id);
				templateDTO.setSteps(hm);
				templateDTO.setTemplateName(template.getTemplateName());
				templateDTO.setTemplateMethod(template.getTemplateMethod());
				templateDTO.setPublishedStatus(template.getPublishedStatus());
				templateDTO.setState(template.getState());
				templateDTO.setTemplateId(template.getTemplateId());

				if (templateDTO != null) {
					logger.info(CLASS + " getTemplateLatestById  res  Template  {}", templateDTO);
					return exceptionHandlerUtil.createSuccessResponse("api.response.template", templateDTO);

				} else {
					return exceptionHandlerUtil.createErrorResponse("api.error.template.not.found");

				}
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.template.is.empty");
			}
		} catch (Exception e) {
			logger.error(CLASS + " getTemplateLatestById  Exception  {}", e.getMessage());
			e.printStackTrace();
			return ExceptionHandlerUtil.handleException(e);
		}

	}

}
