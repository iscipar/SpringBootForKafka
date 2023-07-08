package com.kafka.consumer.deserializers;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class CustomDeserializer implements Deserializer<String> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        try {
            if (data.length == 0) {
                return "Mensaje vacío enviado desde Spring Boot";
            }
            return new String(data).toUpperCase();
        } catch (Exception e) {
            throw new SerializationException("Error al deserializar el mensaje");
        }
    }

    @Override
    public void close() {
    }
}
