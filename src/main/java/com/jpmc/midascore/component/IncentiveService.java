package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {

    private RestTemplate restTemplate = new RestTemplate();

    public float getIncentive(Transaction transaction) {
        try {
            Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                transaction,
                Incentive.class
            );
            if (incentive != null) {
                return incentive.getAmount();
            }
        } catch (Exception e) {
            System.out.println("Incentive API error: " + e.getMessage());
        }
        return 0;
    }
}