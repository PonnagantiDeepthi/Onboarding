package ug.daes.onboarding.service.impl;

import java.awt.image.BufferedImage;
import java.io.*;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

import javax.imageio.ImageIO;

import org.imgscalr.Scalr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.MessageSource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import ug.daes.onboarding.config.SentryClientExceptions;
import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.dto.DocumentResponse;
import ug.daes.onboarding.dto.FileUploadDTO;
import ug.daes.onboarding.dto.Selfie;
import ug.daes.onboarding.exceptions.ExceptionHandlerUtil;
import ug.daes.onboarding.exceptions.OnBoardingServiceException;
import ug.daes.onboarding.model.OnboardingLiveliness;
import ug.daes.onboarding.repository.OnboardingLivelinessRepository;
import ug.daes.onboarding.response.OnBoardingServiceResponse;
import ug.daes.onboarding.util.AppUtil;
import ug.daes.onboarding.util.Utility;

@Service
public class EdmsServiceImpl {
	private static Logger logger = LoggerFactory.getLogger(EdmsServiceImpl.class);

	/** The Constant CLASS. */
	final static String CLASS = "EdmsServiceImpl";

	@Value("${edms.localurl}")
	private String baselocalUrl;

	@Value("${edms.downloadurl}")
	private String edmsDwonlodUrl;

	@Autowired
	private RestTemplate restTemplate;

	@Autowired
	OnboardingLivelinessRepository onboardingLivelinessRepository;

	private static Path testFile;

	@Autowired
	MessageSource messageSource;

	@Autowired
	SentryClientExceptions sentryClientExceptions;

	@Autowired
	OnBoardingServiceResponse onBoardingServiceResponse;

	@Autowired
	OnBoardingServiceException onBoardingServiceException;

	@Autowired
	ExceptionHandlerUtil exceptionHandlerUtil;

	public ApiResponse saveSelfieToEdms(Selfie image) {
		try {
			logger.info(CLASS + "saveSelfieToEdms req for saveSelfieToEdms {}", image.getSubscriberUniqueId());
			byte[] img = Base64.getDecoder().decode(image.getSubscriberSelfie());
			Resource fileRes = getTestFile(img, "selfie", ".jpeg");
			String docIdUrl = baselocalUrl + "/documents";
			MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);
			logger.info(CLASS + " saveSelfieToEdms req for get DocId docIdUrl {} and requestEntity {} ", docIdUrl,
					requestEntity);
			ResponseEntity<DocumentResponse> documentId = restTemplate.exchange(docIdUrl, HttpMethod.POST,
					requestEntity, DocumentResponse.class);
			logger.info(CLASS + " saveSelfieToEdms res for get DocId {}", documentId);
			String docIdAndFileUrl = baselocalUrl + "/documents/" + documentId.getBody().getId() + "/files";
			MultiValueMap<String, Object> bodyMap = new LinkedMultiValueMap<>();
			bodyMap.add("file_new", fileRes);
			bodyMap.add("model", image.getSubscriberUniqueId() + " _Selfie " + AppUtil.getDate());
			bodyMap.add("action", 1);
			HttpHeaders headers4 = new HttpHeaders();
			headers4.setContentType(MediaType.MULTIPART_FORM_DATA);
			HttpEntity<MultiValueMap<String, Object>> requestEntity4 = new HttpEntity<>(bodyMap, headers4);

			logger.info(CLASS + " saveSelfieToEdms req for saveFileWithDocId docIdAndFileUrl {} and requestEntity4 {}",
					docIdAndFileUrl, requestEntity4);
			ResponseEntity<ApiResponse> result = restTemplate.exchange(docIdAndFileUrl, HttpMethod.POST, requestEntity4,
					ApiResponse.class);
			logger.info(CLASS + " saveSelfieToEdms res for saveFileWithDocId {}", result);
			if (result.getStatusCodeValue() == 202) {
				String downloadurlselfie = edmsDwonlodUrl + documentId.getBody().getId() + "/files/downloads";
				if (downloadurlselfie != null) {
					File deleteFile = new File(testFile.toString());
					deleteFile.delete();
					logger.info(CLASS + " saveSelfieToEdms downloadurlselfie {}", downloadurlselfie);

					return exceptionHandlerUtil.createSuccessResponse("api.response.selfie.uploaded.successfully",
							downloadurlselfie);
				}
			} else if (result.getStatusCodeValue() == 500) {
				return exceptionHandlerUtil.createErrorResponseWithResult("api.error.internal.server.error",
						result.getStatusCodeValue());

			} else if (result.getStatusCodeValue() == 400) {
				return exceptionHandlerUtil.createErrorResponseWithResult("api.error.bad.request",
						result.getStatusCodeValue());

			} else if (result.getStatusCodeValue() == 401) {
				return exceptionHandlerUtil.createErrorResponseWithResult("api.error.unauthorized",
						result.getStatusCodeValue());

			} else if (result.getStatusCodeValue() == 403) {
				return exceptionHandlerUtil.createErrorResponseWithResult("api.error.forbidden",
						result.getStatusCodeValue());

			} else if (result.getStatusCodeValue() == 408) {
				return exceptionHandlerUtil.createErrorResponseWithResult("api.error.request.timeout",
						result.getStatusCodeValue());

			} else {
				return exceptionHandlerUtil.createErrorResponseWithResult(
						"api.error.something.went.wrong.please.try.after.sometime", result.getStatusCodeValue());

			}
			return exceptionHandlerUtil.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + "saveSelfieToEdms Exception {}", e.getMessage());
			return exceptionHandlerUtil.handleHttpException(e);
		}

	}

	@Async
	public CompletableFuture<ApiResponse> saveFileToEdms(Object fileContent, String fileType,
			FileUploadDTO fileupload) {
		try {
			logger.info("{}{} - Request to save file to EDMS with fileType: {} and fileUpload: {}", 
				    CLASS, Utility.getMethodName(), fileType, fileupload);
			// Check if the file content is valid (video or selfie)
			if (fileContent == null
					|| (fileContent instanceof MultipartFile && ((MultipartFile) fileContent).isEmpty())) {

				return CompletableFuture.completedFuture(
						exceptionHandlerUtil.createErrorResponse("api.error.file.cant.be.null.or.empty"));
			}
			// Handle different file types (video or selfie)
			if ("video".equals(fileType)) {
				return handleVideoUpload((MultipartFile) fileContent, fileupload);
			} else if ("selfie".equals(fileType)) {
				return handleSelfieUpload((Selfie) fileContent);
			} else {
				return CompletableFuture
						.completedFuture(exceptionHandlerUtil.createErrorResponse("api.error.invalid.file.type"));
			}
		} catch (Exception e) {
			logger.error("{} saveFileToEdms Exception: {}", CLASS, e.getMessage());
			e.printStackTrace();
			return CompletableFuture.completedFuture(ExceptionHandlerUtil.handleException(e));
		}
	}

	public CompletableFuture<ApiResponse> handleVideoUpload(MultipartFile file, FileUploadDTO fileupload) {
		String contentType = file.getContentType();
		logger.info("handleVideoUpload :: file.getContentType() :: {}", contentType);
		// Validate content type for video
		if (contentType == null || !contentType.startsWith("video/")) {
			return CompletableFuture
					.completedFuture(exceptionHandlerUtil.successResponse("api.error.video.content.type.is.not.mp4"));

		}
		// Get Document ID asynchronously and upload video
		return fetchDocumentIdAsync().thenCompose(documentId -> {
			String docIdAndFileUrl = baselocalUrl + "/documents/" + documentId.getId() + "/files";
			logger.info("{} - {} - handleVideoUpload: docIdAndFileUrl: {}", CLASS, Utility.getMethodName(), docIdAndFileUrl);

			return uploadFileAsync(docIdAndFileUrl, file, fileupload).thenApply(result -> {
				if (result.getStatusCodeValue() == 202) {
					String downloadUrl = edmsDwonlodUrl + documentId.getId() + "/files/downloads";
					logger.info("{} - {} - handleVideoUpload: downloadUrl: {}", CLASS, Utility.getMethodName(), downloadUrl);
					saveOnboardingLiveliness(fileupload, downloadUrl);
					return exceptionHandlerUtil.successResponse("api.response.video.uploaded.successfully");
				} else {
					return exceptionHandlerUtil.handleErrorRestTemplateResponse(result.getStatusCodeValue());

				}
			});
		});
	}


	
	public CompletableFuture<ApiResponse> handleSelfieUpload(Selfie image) throws IOException {
	    byte[] img = Base64.getDecoder().decode(image.getSubscriberSelfie());
	    Resource fileRes = getTestFile(img, "selfie", ".jpeg"); // This creates a temp file

	    return fetchDocumentIdAsync().thenCompose(documentId -> {
	        String docIdAndFileUrl = baselocalUrl + "/documents/" + documentId.getId() + "/files";
	        logger.info("{} - {} - fetchDocumentIdAsync: docIdAndFileUrl: {}", CLASS, Utility.getMethodName(), docIdAndFileUrl);

	        MultiValueMap<String, Object> bodyMap = new LinkedMultiValueMap<>();
	        bodyMap.add("file_new", fileRes);
	        bodyMap.add("model", image.getSubscriberUniqueId() + " _Selfie " + AppUtil.getDate());
	        bodyMap.add("action", 1);

	        HttpHeaders headers = new HttpHeaders();
	        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
	        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(bodyMap, headers);

	        return CompletableFuture.supplyAsync(() ->
	            restTemplate.exchange(docIdAndFileUrl, HttpMethod.POST, requestEntity, ApiResponse.class)
	        ).thenApply(result -> {
	            if (result.getStatusCodeValue() == 202) {
	                // ✅ SUCCESS: Delete temp file
	                if (fileRes instanceof FileSystemResource) {
	                    File tempFile = ((FileSystemResource) fileRes).getFile();
	                    if (tempFile.exists()) {
	                        boolean deleted = tempFile.delete();
	                        logger.info("{} - Temp file deleted: {}", Utility.getMethodName(), deleted);
	                    }
	                }

	                String downloadUrl = edmsDwonlodUrl + documentId.getId() + "/files/downloads";
	                logger.info("{} - {} - handleSelfieUpload: downloadUrl: {}", CLASS, Utility.getMethodName(), downloadUrl);
	                return exceptionHandlerUtil.createSuccessResponse("api.response.selfie.uploaded.successfully", downloadUrl);
	            } else {
	                return exceptionHandlerUtil.handleErrorRestTemplateResponse(result.getStatusCodeValue());
	            }
	        });
	    });
	}




	@Async
	public CompletableFuture<DocumentResponse> fetchDocumentIdAsync() {
		String docIdUrl = baselocalUrl + "/documents";
		logger.info("{} - {} - fetchDocumentIdAsync: downloadUrl: {}", CLASS, Utility.getMethodName(), docIdUrl);
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(new LinkedMultiValueMap<>(),
				headers);
		return CompletableFuture.supplyAsync(() -> restTemplate
				.exchange(docIdUrl, HttpMethod.POST, requestEntity, DocumentResponse.class).getBody());
	}
	
	@Async
	public CompletableFuture<ResponseEntity<ApiResponse>> uploadFileAsync(
	        String docIdAndFileUrl, MultipartFile file, FileUploadDTO fileupload) {

	    MultiValueMap<String, Object> bodyMap = new LinkedMultiValueMap<>();
	    
	    File convertedFile = convert(file);
	    System.out.println(" convertedFile size 22222222::"+convertedFile.length());
	    
	    HttpHeaders headers = new HttpHeaders();
	    headers.setContentType(MediaType.APPLICATION_OCTET_STREAM); // or MediaType.MULTIPART_FORM_DATA
	    HttpEntity<FileSystemResource> fileEntity = new HttpEntity<>(new FileSystemResource(convertedFile), headers);

	    //bodyMap.add("fileNew", fileEntity);
		bodyMap.add("file_new", fileEntity);  // match the controller exactly

		bodyMap.add("file_new", new FileSystemResource(convertedFile));
	    bodyMap.add("model", fileupload.getSubscriberUid() + " _Video " + AppUtil.getDate());

	   // HttpHeaders headers = new HttpHeaders();
	    headers.setContentType(MediaType.MULTIPART_FORM_DATA);
	   // headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

	    HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(bodyMap, headers);

	    return CompletableFuture.supplyAsync(() -> {
	        try {
	            return restTemplate.exchange(docIdAndFileUrl, HttpMethod.POST, requestEntity, ApiResponse.class);
	        } catch (HttpClientErrorException e) {
	            e.printStackTrace();

	            ApiResponse errorResponse = new ApiResponse();
	            errorResponse.setSuccess(false);
	            errorResponse.setMessage("Upload failed: " + e.getStatusCode() + " - " + e.getResponseBodyAsString());

	            return ResponseEntity.status(e.getStatusCode()).body(errorResponse);
	        } catch (Exception e) {
	            e.printStackTrace();

	            ApiResponse errorResponse = new ApiResponse();
	            errorResponse.setSuccess(false);
	            errorResponse.setMessage("File upload failed: " + e.getMessage());

	            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
	        }
	    });
	}


//	@Async
//	public CompletableFuture<ResponseEntity<ApiResponse>> uploadFileAsync(String docIdAndFileUrl, MultipartFile file,
//			FileUploadDTO fileupload) {
//		MultiValueMap<String, Object> bodyMap = new LinkedMultiValueMap<>();
//		bodyMap.add("file", new FileSystemResource(convert(file)));
//		bodyMap.add("model", fileupload.getSubscriberUid() + " _Video " + AppUtil.getDate());
//		//bodyMap.add("action", 1);
//
//		HttpHeaders headers = new HttpHeaders();
//		headers.setContentType(MediaType.MULTIPART_FORM_DATA);
//		headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
//
//		HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(bodyMap, headers);
//		return CompletableFuture.supplyAsync(() -> {
//	        try {
//	            return restTemplate.exchange(docIdAndFileUrl, HttpMethod.POST, requestEntity, ApiResponse.class);
//	        } catch (Exception e) {
//	            e.printStackTrace(); // log the exception
//
//	            ApiResponse errorResponse = new ApiResponse();
//	            // Assuming your ApiResponse has these fields:
//	            errorResponse.setSuccess(false);
//	            errorResponse.setMessage("File upload failed: " + e.getMessage());
//
//	            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
//	        }
//	    });
//		//return CompletableFuture.supplyAsync(
//			//	() -> restTemplate.exchange(docIdAndFileUrl, HttpMethod.POST, requestEntity, ApiResponse.class));
//	}

	public void saveOnboardingLiveliness(FileUploadDTO fileUploadDTO, String downloadUrl) {
		OnboardingLiveliness onboardingLiveliness = new OnboardingLiveliness();
		onboardingLiveliness.setSubscriberUid(fileUploadDTO.getSubscriberUid());
		onboardingLiveliness.setRecordedTime(fileUploadDTO.getRecordedTime());
		onboardingLiveliness.setRecordedGeoLocation(fileUploadDTO.getRecordedGeoLocation());
		onboardingLiveliness.setVerificationFirst(fileUploadDTO.getVerificationFirst().name());
		onboardingLiveliness.setVerificationSecond(fileUploadDTO.getVerificationSecond().name());
		onboardingLiveliness.setVerificationThird(fileUploadDTO.getVerificationThird().name());
		onboardingLiveliness.setTypeOfService(fileUploadDTO.getTypeOfService().name());
		onboardingLiveliness.setUrl(downloadUrl);
		onboardingLivelinessRepository.save(onboardingLiveliness);
	}

//	@Async
//	public CompletableFuture<ApiResponse> createThumbnailOfSelfie(Selfie image) throws IOException {
//		try {
//			if (image != null) {
//				byte[] img = Base64.getDecoder().decode(image.getSubscriberSelfie());
//				Resource fileRes = getTestFile(img, "selfieThumbnail", ".jpeg");
//				ByteArrayOutputStream thumbOutput = new ByteArrayOutputStream();
//				BufferedImage thumbImg = null;
//				BufferedImage img2 = ImageIO.read(fileRes.getInputStream());
//				thumbImg = Scalr.resize(img2, Scalr.Method.AUTOMATIC, Scalr.Mode.AUTOMATIC, 100, Scalr.OP_ANTIALIAS);
//				ImageIO.write(thumbImg, "jpeg", thumbOutput);
//				byte[] data = thumbOutput.toByteArray();
//				String base64EncodedImageBytes = Base64.getEncoder().encodeToString(data);
//				logger.info("{}{} - Selfie Thumbnail Generated Successfully", CLASS, Utility.getMethodName());
//				return CompletableFuture.completedFuture(exceptionHandlerUtil.createSuccessResponse(
//						"api.response.selfie.thumbnail.generated.successfully", base64EncodedImageBytes));
//			} else {
//				return CompletableFuture.completedFuture(
//						exceptionHandlerUtil.createErrorResponse("api.error.selfie.cant.be.null.or.empty"));
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//			logger.error("{}{} - Exception while creating selfie thumbnail: {}", CLASS, Utility.getMethodName(), e.getMessage());
//			return CompletableFuture.completedFuture(ExceptionHandlerUtil.handleException(e));
//
//		}
//	}

	@Async
	public CompletableFuture<ApiResponse> createThumbnailOfSelfie(Selfie image) {
		try {
			if (image != null) {
				byte[] imgBytes = Base64.getDecoder().decode(image.getSubscriberSelfie());
				InputStream imgInputStream = new ByteArrayInputStream(imgBytes);

				BufferedImage originalImage = ImageIO.read(imgInputStream);
				if (originalImage == null) {
					return CompletableFuture.completedFuture(AppUtil.createApiResponse(false,
							messageSource.getMessage("api.error.invalid.image.format", null, Locale.ENGLISH), null));
				}

				BufferedImage thumbnail = Scalr.resize(originalImage, Scalr.Method.AUTOMATIC, Scalr.Mode.AUTOMATIC, 100,
						Scalr.OP_ANTIALIAS);

				ByteArrayOutputStream thumbOutput = new ByteArrayOutputStream();
				ImageIO.write(thumbnail, "jpeg", thumbOutput);

				byte[] thumbnailBytes = thumbOutput.toByteArray();
				String base64EncodedThumbnail = Base64.getEncoder().encodeToString(thumbnailBytes);

				logger.info(CLASS + " createThumbnailOfSelfie: Selfie Thumbnail Generated Successfully");

				return CompletableFuture.completedFuture(AppUtil.createApiResponse(true, messageSource
								.getMessage("api.response.selfie.thumbnail.generated.successfully", null, Locale.ENGLISH),
						base64EncodedThumbnail));
			} else {
				return CompletableFuture.completedFuture(AppUtil.createApiResponse(false,
						messageSource.getMessage("api.error.selfie.cant.be.null.or.empty", null, Locale.ENGLISH),
						null));
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + " createThumbnailOfSelfie Exception {}", e.getMessage());
			return CompletableFuture.completedFuture(AppUtil.createApiResponse(false, messageSource.getMessage(
					"api.error.something.went.wrong.please.try.after.sometime", null, Locale.ENGLISH), null));
		}
	}


	public ApiResponse createThumlbnailOfSelfie(Selfie image) throws IOException {
		try {
			if (image != null) {
				logger.info(CLASS + " createThumlbnailOfSelfie ");
				byte[] img = Base64.getDecoder().decode(image.getSubscriberSelfie());
				Resource fileRes = getTestFile(img, "selfieThumbnail", ".jpeg");

				ByteArrayOutputStream thumbOutput = new ByteArrayOutputStream();
				BufferedImage thumbImg = null;
				BufferedImage img2 = ImageIO.read(fileRes.getInputStream());
				thumbImg = Scalr.resize(img2, Scalr.Method.AUTOMATIC, Scalr.Mode.AUTOMATIC, 100, Scalr.OP_ANTIALIAS);
				ImageIO.write(thumbImg, "jpeg", thumbOutput);
				byte[] data = thumbOutput.toByteArray();
				String base64EncodedImageBytes = Base64.getEncoder().encodeToString(data);
				logger.info(CLASS + " createThumlbnailOfSelfie Selfie Thumbnail Genrated Succssfully ");
				return exceptionHandlerUtil.createSuccessResponse("api.response.selfie.thumbnail.genrated.succssfully",
						base64EncodedImageBytes);
			} else {
				return exceptionHandlerUtil.createErrorResponse("api.error.selfie.cant.be.null.or.empty");
			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + " createThumlbnailOfSelfie Exception {}", e.getMessage());
			return ExceptionHandlerUtil.handleException(e);
		}
	}

	public ApiResponse saveVideo(MultipartFile file, FileUploadDTO fileUpload) throws UnknownHostException {
		logger.info("{}{} - Save Video file: {} and fileUpload: {}", CLASS, Utility.getMethodName(), file, fileUpload);
		if (!(file.isEmpty()) && fileUpload.getSubscriberUid() != null) {
			return saveVideoToEdms(file, fileUpload);
		}
		return exceptionHandlerUtil.createFailedResponseWithCustomMessage("", null);
	}

	public ApiResponse saveVideoToEdms(MultipartFile file, FileUploadDTO fileupload) throws UnknownHostException {
		try {
			logger.info(CLASS + " saveVideoToEdms req fileupload {} and File {} ", fileupload,
					file.getOriginalFilename());

			if (file.isEmpty()) {
				return exceptionHandlerUtil.createErrorResponse("api.error.video.cant.be.null.or.empty");
			}

			String contentType = file.getContentType();
			if (contentType == null && !contentType.startsWith("video/")) {
				return exceptionHandlerUtil.createErrorResponse("api.error.vedio.content.type.isnot.mp4");
			}

			String docIdUrl = baselocalUrl + "/documents";
			MultiValueMap<String, Object> bodyMap = new LinkedMultiValueMap<>();
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(bodyMap, headers);
			logger.info(CLASS + " saveVideoToEdms req for get DocId docIdUrl {} and requestEntity {}", docIdUrl,
					requestEntity);
			ResponseEntity<DocumentResponse> documentId = restTemplate.exchange(docIdUrl, HttpMethod.POST,
					requestEntity, DocumentResponse.class);
			logger.info(CLASS + " saveVideoToEdms res for get DocId {}", documentId);
			String docIdAndFileUrl = baselocalUrl + "/documents/" + documentId.getBody().getId() + "/files";
			MultiValueMap<String, Object> bodyMap1 = new LinkedMultiValueMap<>();
			bodyMap1.add("file_new", new FileSystemResource(convert(file)));
			// bodyMap1.add("file_new", new FileSystemResource(convert(file)));
			// bodyMap1.add("file_new", convertMultipartFileToFile(file));
			bodyMap1.add("model", fileupload.getSubscriberUid() + " _Video " + AppUtil.getDate());
			bodyMap1.add("action", 1);
			HttpHeaders headers1 = new HttpHeaders();
			headers1.setContentType(MediaType.MULTIPART_FORM_DATA);
			HttpEntity<MultiValueMap<String, Object>> requestEntity1 = new HttpEntity<>(bodyMap1, headers1);
			logger.info(CLASS + " saveVideoToEdms req for saveFileWithDocId docIdAndFileUrl {} and requestEntity1 {}",
					docIdAndFileUrl, requestEntity1);
			ResponseEntity<ApiResponse> result = restTemplate.exchange(docIdAndFileUrl, HttpMethod.POST, requestEntity1,
					ApiResponse.class);
			logger.info(CLASS + " saveVideoToEdms res for saveFileWithDocId {}", result);
			if (result.getStatusCodeValue() == 202) {
				String download = edmsDwonlodUrl + documentId.getBody().getId() + "/files/downloads";
				logger.info(CLASS + " saveVideoToEdms downloadVideoUrl {}", download);
				if (download != null) {
					OnboardingLiveliness onboardingLiveliness = new OnboardingLiveliness();
					onboardingLiveliness.setSubscriberUid(fileupload.getSubscriberUid());
					onboardingLiveliness.setRecordedTime(fileupload.getRecordedTime());
					onboardingLiveliness.setRecordedGeoLocation(fileupload.getRecordedGeoLocation());
					onboardingLiveliness.setVerificationFirst(fileupload.getVerificationFirst().name());
					onboardingLiveliness.setVerificationSecond(fileupload.getVerificationSecond().name());
					onboardingLiveliness.setVerificationThird(fileupload.getVerificationThird().name());
					onboardingLiveliness.setTypeOfService(fileupload.getTypeOfService().name());
					onboardingLiveliness.setUrl(download);
					onboardingLivelinessRepository.save(onboardingLiveliness);
					logger.info(CLASS + " saveVideoToEdms true Video uploaded successfully ");
					return exceptionHandlerUtil.successResponse("api.response.video.uploaded.successfully");
				}
			} else if (result.getStatusCodeValue() == 500) {
				logger.error(CLASS + "saveVideoToEdms false Internal Server Error = 500 ");
				return exceptionHandlerUtil.createErrorResponseWithResult("api.error.internal.server.error",
						result.getStatusCodeValue());

			} else if (result.getStatusCodeValue() == 400) {
				logger.error(CLASS + " saveVideoToEdms false Bad Request = 400 ");
				return exceptionHandlerUtil.createErrorResponseWithResult("api.error.bad.request",
						result.getStatusCodeValue());

			} else if (result.getStatusCodeValue() == 401) {
				logger.error(CLASS + " saveVideoToEdms false Unauthorized = 401 ");
				return exceptionHandlerUtil.createErrorResponseWithResult("api.error.unauthorized",
						result.getStatusCodeValue());

			} else if (result.getStatusCodeValue() == 403) {
				logger.error(CLASS + " saveVideoToEdms false Forbidden = 403");
				return exceptionHandlerUtil.createErrorResponseWithResult("api.error.forbidden",
						result.getStatusCodeValue());

			} else if (result.getStatusCodeValue() == 408) {
				logger.error(CLASS + " saveVideoToEdms false Request Timeout = 408");
				return exceptionHandlerUtil.createErrorResponseWithResult("api.error.request.timeout",
						result.getStatusCodeValue());

			} else {
				logger.error(CLASS + "saveVideoToEdms false Something went wrong. Try after sometime 1");
				return exceptionHandlerUtil.createErrorResponseWithResult(
						"api.error.something.went.wrong.please.try.after.sometime", result.getStatusCodeValue());

			}
			logger.error(CLASS + " saveVideoToEdms false Something went wrong. Try after sometime 2");

			return exceptionHandlerUtil.createErrorResponse("api.error.something.went.wrong.please.try.after.sometime");

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(CLASS + "saveVideoToEdms Exception {}", e.getMessage());
			sentryClientExceptions.captureTags(fileupload.getSubscriberUid(), null, "saveVideoToEdms",
					"VideoUploadUrl");
			sentryClientExceptions.captureExceptions(e);
			return exceptionHandlerUtil.handleHttpException(e);
		}
	}

	public static File convertMultipartFileToFile(MultipartFile multipartFile) throws IOException {
		File file = new File(multipartFile.getName());
		FileOutputStream fos = new FileOutputStream(file);
		fos.write(multipartFile.getBytes());
		fos.close();
		return file;
	}

	public static File convert(MultipartFile file) {
		System.out.println("TOMCAT_HOME_PATH ::" + System.getProperty("catalina.home"));
		//String tomcatBasePath = System.getProperty("catalina.home");
		
		//File folder = new File(System.getProperty("java.io.tmpdir"), "ObTempFiles");
		
		File folder = new File(System.getProperty("catalina.home"), "ObTempFiles");
		
		// Create a File object representing the folder
		//File folder = new File(tomcatBasePath, "ObTempFiles");
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

	public static File convertOLD(MultipartFile file) {
		File convFile = new File(file.getOriginalFilename());
		try {
			System.out.println("FILE SIZE ::" + file.getSize());
			convFile.createNewFile();
			FileOutputStream fos = new FileOutputStream(convFile);
			fos.write(file.getBytes());
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return convFile;
	}

	public static Resource getTestFile(byte[] bytes, String prefix, String suffix) throws IOException {
		testFile = Files.createTempFile(prefix, suffix);
		Files.write(testFile, bytes);

		return new FileSystemResource(testFile.toFile());
	}

}