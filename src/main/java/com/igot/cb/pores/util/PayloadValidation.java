package com.igot.cb.pores.util;

import java.util.Set;

import org.igot.common.CustomException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.ValidationMessage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class PayloadValidation {

  JsonSchemaCache schemaCache;

  public PayloadValidation(JsonSchemaCache schemaCache) {
    this.schemaCache = schemaCache;
  }

  public void validatePayload(String schemaKey, JsonNode payload) {
    try {
      JsonSchema schema = schemaCache.getSchema(schemaKey);

      if (schema == null) {
        String errorMsg = String.format("Schema not found for key: %s", schemaKey);
        throw new CustomException(errorMsg, errorMsg, HttpStatus.BAD_REQUEST);
      }

      if (payload.isArray()) {
        for (JsonNode objectNode : payload) {
          validateObject(schema, objectNode);
        }
      } else {
        validateObject(schema, payload);
      }
    } catch (Exception e) {
      log.error("Failed to validate payload", e);
      throw new CustomException("Failed to validate payload", e.getMessage(), HttpStatus.BAD_REQUEST);
    }
  }

  private void validateObject(JsonSchema schema, JsonNode objectNode) {
    Set<ValidationMessage> validationMessages = schema.validate(objectNode);
    if (!validationMessages.isEmpty()) {
      StringBuilder errorMessage = new StringBuilder("Validation error(s): \n");
      for (ValidationMessage message : validationMessages) {
        errorMessage.append(message.getMessage()).append("\n");
      }
      log.error("Validation Error", errorMessage.toString());
      throw new CustomException("Validation Error", errorMessage.toString(), HttpStatus.BAD_REQUEST);
    }
  }
}