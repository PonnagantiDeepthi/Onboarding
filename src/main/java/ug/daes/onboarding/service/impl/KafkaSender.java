package ug.daes.onboarding.service.impl;



import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ug.daes.onboarding.dto.LogModelDTO;

@Service
public class KafkaSender {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${com.dt.kafka.topic}")
    private String topicName;

    @Value("${com.dt.kafka.topic.central}")
    private String centralTopicName;

    // ✅ Send JSON DTO
    public void send(LogModelDTO logmodel) {
        System.out.println("Kafka -> Sending LogModelDTO to topic: " + topicName);
        System.out.println("Central Topic => " + centralTopicName);
        kafkaTemplate.send(topicName, logmodel);
        // To send to central topic:
         kafkaTemplate.send(centralTopicName, logmodel);
    }

    // ✅ Send String message
    public void sendString(String logmodel) {
        System.out.println("Kafka -> Sending String message to topic: " + topicName);
        kafkaTemplate.send(topicName, logmodel);
        // To send to central topic:
         kafkaTemplate.send(centralTopicName, logmodel);
    }
}
