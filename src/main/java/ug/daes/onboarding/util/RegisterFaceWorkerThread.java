package ug.daes.onboarding.util;


import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import ug.daes.onboarding.constant.ApiResponse;
import ug.daes.onboarding.dto.FaceFeaturesDto;


public class RegisterFaceWorkerThread implements Runnable{

    private static String registerFaceURL;
    private static FaceFeaturesDto faceFeaturesDto;

    public RegisterFaceWorkerThread(String registerFaceURL, FaceFeaturesDto faceFeaturesDto) {
        this.registerFaceURL = registerFaceURL;
        this.faceFeaturesDto = faceFeaturesDto;
    }

    @Override
    public void run() {
        try {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Object> reqEntity = new HttpEntity<>(faceFeaturesDto, headers);
            String url = registerFaceURL;
            // RestTemplate restTemplate3 = new RestTemplate();
            System.out.println(" face url  http://10.4.72.20:8083/register_face +" + url);
            ResponseEntity<ApiResponse> res = restTemplate.exchange(registerFaceURL, HttpMethod.POST, reqEntity,
                    ApiResponse.class);
            if (res.getStatusCodeValue() == 200 || res.getStatusCodeValue() == 201) {
                System.out.println(" face features result : status code 200 ");
            }
        } catch (Exception e) {
            System.out.println(" failed register_face "+e.getMessage());
            e.printStackTrace();
        }
        // TODO Auto-generated method stub

    }

}
