package ug.daes.onboarding.service.impl;

import io.minio.*;
import io.minio.http.Method;
import org.imgscalr.Scalr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.MessageSource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.dto.FileUploadDTO;
import ug.daes.onboarding.dto.Selfie;
import ug.daes.onboarding.exceptions.ExceptionHandlerUtil;
import ug.daes.onboarding.model.OnboardingLiveliness;
import ug.daes.onboarding.repository.OnboardingLivelinessRepository;
import ug.daes.onboarding.util.AppUtil;
import ug.daes.onboarding.util.Utility;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Base64;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

@Service
public class MinioStorageServiceImpl {

    private static final Logger logger = LoggerFactory.getLogger(MinioStorageServiceImpl.class);
    private static final String CLASS = "MinioStorageServiceImpl";

    @Autowired
    private MinioClient minioClient;

    @Value("${minio.bucket.name}")
    private String bucketName;

    @Value("${minio.expiry.days}")
    private int expiryDays;

    @Value("${minio.url}")
    private String minioEndpoint;


    @Value("${app.base.url}")
    private String baseUrl;

    @Autowired
    MessageSource messageSource;

    @Autowired
    ExceptionHandlerUtil exceptionHandlerUtil;

    @Autowired
    OnboardingLivelinessRepository onboardingLivelinessRepository;

    /* =========================================================
     * COMMON UTIL
     * ======================================================== */


    private String generateFileName(String prefix, String extension) {
        String timestamp = new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS")
                .format(new java.util.Date());
        String randomId = java.util.UUID.randomUUID().toString().substring(0, 4); // short unique ID
        return prefix + "_" + timestamp + "_" + randomId + extension;
    }


    private void ensureBucketExists() throws Exception {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        }
    }

    @Async
    public CompletableFuture<ApiResponse> saveFileToMinio(Object fileContent, String fileType, FileUploadDTO fileupload) {
        try {
            logger.info("{}{} - Request to save file to MinIO with fileType: {} and fileUpload: {}",
                    CLASS, Utility.getMethodName(), fileType, fileupload);

            // Validate content
            if (fileContent == null ||
                    (fileContent instanceof MultipartFile && ((MultipartFile) fileContent).isEmpty())) {

                return CompletableFuture.completedFuture(
                        exceptionHandlerUtil.createErrorResponse("api.error.file.cant.be.null.or.empty"));
            }

            // Handle file based on type
            if ("video".equalsIgnoreCase(fileType) && fileContent instanceof MultipartFile) {
                return uploadVideo((MultipartFile) fileContent, fileupload);
            }
            else if ("selfie".equalsIgnoreCase(fileType) && fileContent instanceof Selfie) {
                return uploadSelfie((Selfie) fileContent);
            }
            else {
                return CompletableFuture.completedFuture(
                        exceptionHandlerUtil.createErrorResponse("api.error.invalid.file.type"));
            }

        } catch (Exception e) {
            logger.error("{} saveFileToMinio Exception: {}", CLASS, e.getMessage(), e);
            return CompletableFuture.completedFuture(exceptionHandlerUtil.handleException(e));
        }
    }

    private ApiResponse uploadFile(InputStream inputStream, long size, String path, String contentType) {
        try {
            ensureBucketExists();

            // Upload file
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(path)
                            .stream(inputStream, size, -1)
                            .contentType(contentType)
                            .build()
            );

            // Generate presigned URL
            String presignedUrl = minioClient.getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .method(Method.GET)
                            .bucket(bucketName)
                            .object(path)
                            .expiry(expiryDays * 24 * 3600)
                            .build()
            );

            // Return response
            return AppUtil.createApiResponse(true, "File uploaded successfully", presignedUrl);

        } catch (Exception e) {
            // Handle error and return error response
            return  AppUtil.createApiResponse(false, "File upload failed: " + e.getMessage(), null);
        }
    }


    /* =========================================================
     * SELFIE UPLOAD
     * ======================================================== */

    @Async
    public CompletableFuture<ApiResponse> uploadSelfie(Selfie image) {
        try {
            byte[] img = Base64.getDecoder().decode(image.getSubscriberSelfie());
            if (img.length == 0) {
                return CompletableFuture.completedFuture(exceptionHandlerUtil.createErrorResponse("api.error.file.cant.be.null.or.empty"));
            }

            String fileName = generateFileName("selfie",".jpeg");
            String path = image.getSubscriberUniqueId() + "/selfie/" + fileName;

            ApiResponse res = uploadFile(new ByteArrayInputStream(img), img.length, path, "image/jpeg");
            if(!res.isSuccess()){
                return CompletableFuture.completedFuture(exceptionHandlerUtil.createErrorResponse("api.error.selfie.upload.failed", null));

            }

            CompletableFuture<ApiResponse> selfieURI = generateSelfieURI(image.getSubscriberUniqueId(), fileName);
            return CompletableFuture.completedFuture(exceptionHandlerUtil.createSuccessResponse("api.response.selfie.uploaded.successfully", selfieURI.get().getResult()));
        } catch (Exception e) {
            logger.error(CLASS + " uploadSelfie Exception {}", e.getMessage());
            return CompletableFuture.completedFuture(exceptionHandlerUtil.handleException(e));
        }
    }

    /* =========================================================
     * VIDEO UPLOAD
     * ======================================================== */

    @Async
    public CompletableFuture<ApiResponse> uploadVideo(MultipartFile file, FileUploadDTO fileupload) {
        try {
            if (file.isEmpty() || fileupload.getSubscriberUid() == null) {
                return CompletableFuture.completedFuture(exceptionHandlerUtil.createErrorResponse("api.error.video.cant.be.null.or.empty"));
            }

            if (!file.getContentType().startsWith("video/")) {
                return CompletableFuture.completedFuture(exceptionHandlerUtil.createErrorResponse("api.error.video.content.type.is.not.mp4"));
            }

            File tempFile = convertTempFile(file);
            String fileName = generateFileName("video", ".mp4");
            String path = fileupload.getSubscriberUid() + "/video/" + fileName;

            ApiResponse res = uploadFile(new FileInputStream(tempFile), tempFile.length(), path, "video/mp4");
            tempFile.delete();

            if(!res.isSuccess()){
                return CompletableFuture.completedFuture(exceptionHandlerUtil.createErrorResponse("api.error.video.upload.failed", null));

            }

            CompletableFuture<ApiResponse> videoURI = generateVideoURI(fileupload.getSubscriberUid(), fileName);


            saveOnboardingLiveliness(fileupload, videoURI.get().getResult().toString());

            return CompletableFuture.completedFuture(exceptionHandlerUtil.successResponse("api.response.video.uploaded.successfully"));
        } catch (Exception e) {
            logger.error(CLASS + " uploadVideo Exception {}", e.getMessage());
            return CompletableFuture.completedFuture(exceptionHandlerUtil.handleException(e));
        }
    }

    /* =========================================================
     * FILE DELETE
     * ======================================================== */

    public ApiResponse deleteFile(String subscriberUid, String folder, String fileName) {
        try {
            String path = subscriberUid + "/" + folder + "/" + fileName;

            minioClient.removeObject(
                    RemoveObjectArgs.builder()
                            .bucket(bucketName)
                            .object(path)
                            .build()
            );

            return exceptionHandlerUtil.successResponse("File deleted successfully");
        } catch (Exception e) {
            logger.error(CLASS + " deleteFile Exception {}", e.getMessage());
            return exceptionHandlerUtil.handleException(e);
        }
    }

    /* =========================================================
     * FILE DOWNLOAD
     * ======================================================== */

    public ApiResponse generateDownloadUrl(String subscriberUid, String folder, String fileName) {
        try {
            String path = subscriberUid + "/" + folder + "/" + fileName;

            String url = minioClient.getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .method(Method.GET)
                            .bucket(bucketName)
                            .object(path)
                            .expiry(expiryDays * 24 * 3600)
                            .build()
            );

            return exceptionHandlerUtil.createSuccessResponse("File download URL generated", url);
        } catch (Exception e) {
            logger.error(CLASS + " generateDownloadUrl Exception {}", e.getMessage());
            return exceptionHandlerUtil.handleException(e);
        }
    }

    /* =========================================================
     * THUMBNAIL PROCESSING
     * ======================================================== */


    //new one // uri will expire


    @Async
    public CompletableFuture<ApiResponse> generateSelfieURI(String subscriberUid, String fileName) {
        try {
            if (subscriberUid == null || fileName == null) {
                return CompletableFuture.completedFuture(
                        exceptionHandlerUtil.createErrorResponse("api.error.invalid.thumbnail.request")
                );
            }

            // Build download API link (just like EDMS download behavior)
            String downloadUrl = baseUrl + "/api/documents/" + subscriberUid + "/selfie/" + fileName + "/download";

            System.out.println("==== Download URL Generated ====");
            System.out.println("Selfie URL: " + downloadUrl);
            System.out.println("==========================================");

            // Return the URL (do NOT create presigned MinIO link)
            return CompletableFuture.completedFuture(
                    exceptionHandlerUtil.createSuccessResponse(
                            "api.response.selfie.thumbnail.generated.successfully",
                            downloadUrl
                    )
            );

        } catch (Exception e) {
            e.printStackTrace();
            return CompletableFuture.completedFuture(
                    exceptionHandlerUtil.handleException(e)
            );
        }
    }

    @Async
    public CompletableFuture<ApiResponse> generateVideoURI(String subscriberUid, String fileName) {
        try {
            if (subscriberUid == null || fileName == null) {
                return CompletableFuture.completedFuture(
                        exceptionHandlerUtil.createErrorResponse("api.error.invalid.video.uri.request")
                );
            }

            // Build download API link (similar to EDMS)
            String downloadUrl = baseUrl + "/api/documents/" + subscriberUid + "/video/" + fileName + "/download";

            System.out.println("==== Video Download URL Generated ====");
            System.out.println("Video URL: " + downloadUrl);
            System.out.println("======================================");

            return CompletableFuture.completedFuture(
                    exceptionHandlerUtil.createSuccessResponse(
                            "api.response.video.uri.generated.successfully",
                            downloadUrl
                    )
            );

        } catch (Exception e) {
            e.printStackTrace();
            return CompletableFuture.completedFuture(
                    exceptionHandlerUtil.handleException(e)
            );
        }
    }


    /* =========================================================
     * UTILS
     * ======================================================== */

    private File convertTempFile(MultipartFile file) throws IOException {
        File temp = new File(System.getProperty("java.io.tmpdir"), file.getOriginalFilename());
        try (FileOutputStream fos = new FileOutputStream(temp)) {
            fos.write(file.getBytes());
        }
        return temp;
    }

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

    private void saveOnboardingLiveliness(FileUploadDTO dto, String url) {
        OnboardingLiveliness entity = new OnboardingLiveliness();
        entity.setSubscriberUid(dto.getSubscriberUid());
        entity.setRecordedTime(dto.getRecordedTime());
        entity.setRecordedGeoLocation(dto.getRecordedGeoLocation());
        entity.setVerificationFirst(dto.getVerificationFirst().name());
        entity.setVerificationSecond(dto.getVerificationSecond().name());
        entity.setVerificationThird(dto.getVerificationThird().name());
        entity.setTypeOfService(dto.getTypeOfService().name());
        entity.setUrl(url);
        onboardingLivelinessRepository.save(entity);
    }
}
