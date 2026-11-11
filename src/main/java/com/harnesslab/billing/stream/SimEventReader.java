package com.harnesslab.billing.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.harnesslab.billing.types.MalformedEventException;
import com.harnesslab.billing.types.SimActivated;
import com.harnesslab.billing.types.SimDeactivated;
import java.io.IOException;
import org.springframework.stereotype.Component;

/**
 * Turns sim-service message bytes into records, following docs/contracts in sim-service. Fields billing does
 * not use are ignored; a missing field billing needs makes the message malformed.
 */
@Component
public class SimEventReader {

    private final ObjectMapper objectMapper;

    public SimEventReader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public SimActivated activated(byte[] value) {
        SimActivated event = read(value, SimActivated.class);
        requireFields(SimActivated.TOPIC, event.eventId(), event.iccid(), event.msisdn(), event.activatedAt());
        return event;
    }

    public SimDeactivated deactivated(byte[] value) {
        SimDeactivated event = read(value, SimDeactivated.class);
        requireFields(SimDeactivated.TOPIC, event.eventId(), event.iccid(), event.msisdn(), event.deactivatedAt());
        return event;
    }

    private <T> T read(byte[] value, Class<T> type) {
        if (value == null) {
            throw new MalformedEventException("Message has no value", null);
        }
        try {
            return objectMapper.readValue(value, type);
        } catch (IOException exception) {
            throw new MalformedEventException("Cannot read " + type.getSimpleName() + ": " + exception.getMessage(), exception);
        }
    }

    private static void requireFields(String topic, Object... fields) {
        for (Object field : fields) {
            if (field == null) {
                throw new MalformedEventException(topic + " message misses a required field", null);
            }
        }
    }
}
