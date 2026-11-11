package com.harnesslab.billing.config;

import com.harnesslab.billing.types.ActivationFee;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BillingConfiguration {

    @Bean
    ActivationFee activationFee(
            @Value("${billing.activation-fee.amount-minor}") long amountMinor,
            @Value("${billing.activation-fee.currency}") String currency) {
        return new ActivationFee(amountMinor, currency);
    }
}
