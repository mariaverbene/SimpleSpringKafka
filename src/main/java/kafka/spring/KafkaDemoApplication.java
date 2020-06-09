package kafka.spring;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;
import java.util.Scanner;

@SpringBootApplication
public class KafkaDemoApplication implements ApplicationRunner {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public static void main(String[] args)  {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        System.out.println("Enter a message");
        String msg = new Scanner(System.in).nextLine();
       // String msg = "{\"firstName\": \"Sergey\", \"lastName\": \"Prokofiev\", \"age\": 29}";
        kafkaTemplate.send("topic1", msg);
    }

    @KafkaListener(topics = "topic1")
    public void listen(ConsumerRecord record) throws IOException {
        String message = (String) record.value();
        System.out.println("Reading message from topic1: " + message);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(message);
        ((ObjectNode) jsonNode).put("handledTimestamp", record.timestamp());
        message = mapper.writeValueAsString(jsonNode);

        System.out.println("Adjusted message" + message);

        kafkaTemplate.send("topic2", message);
    }


}