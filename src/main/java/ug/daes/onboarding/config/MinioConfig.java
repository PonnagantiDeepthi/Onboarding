package ug.daes.onboarding.config;

import io.minio.MinioClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MinioConfig {

    @Value("${minio.url}")
    private String minioUrl;

    @Value("${minio.access.key}")
    private String accessKey;

    @Value("${minio.secret.key}")
    private String secretKey;

    @Value("${minio.secure:false}")  // default false if not provided
    private boolean secure;

    @Bean
    public MinioClient minioClient() {
//        String protocol = secure ? "https://" : "http://";

        System.out.println("MINIO CONFIGURATION");
        System.out.println("Endpoint: " + minioUrl);
        System.out.println("Access Key: " + accessKey);
        System.out.println("Secure: " + secure);

        return MinioClient.builder()
                .endpoint(minioUrl)
                .credentials(accessKey, secretKey)
                .build();
    }
}
