package com.igot.cb.profanity.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.discussion.repository.DiscussionRepository;
import com.igot.cb.pores.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class ProfanityConsumer {

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private DiscussionRepository discussionRepository;

    /**
     * Kafka listener for processing profanity check messages.
     * Extracts contentId and isProfane from the message and updates the discussion record.
     *
     * @param textData the Kafka message containing JSON data
     */
    @KafkaListener(topics = "${kafka.topic.process.check.content.profanity}", groupId = "${kafka.group.process.check.content.profanity}")
    public void checkTextContentIsProfane(ConsumerRecord<String, String> textData) {
        if (StringUtils.hasText(textData.value())) {
            try {
                JsonNode textDataNode = mapper.readTree(textData.value());
                String postId = textDataNode.path(Constants.REQUEST_DATA).path(Constants.METADATA).path(Constants.POST_ID).asText();
                boolean isProfane = textDataNode.path(Constants.RESPONSE_DATA).path(Constants.RESPONSE_DATA_PATH).path(Constants.IS_PROFANE).asBoolean(false);
                String profanityResponseJson = textDataNode.toString();
                // Update the discussion record asynchronously
                CompletableFuture.runAsync(() -> {
                    try {
                        discussionRepository.updateProfanityFieldsByDiscussionId(postId, profanityResponseJson, isProfane);
                        log.info("Successfully updated profanity fields for PostId: {}", postId);
                    } catch (Exception ex) {
                        log.error("Failed to update profanity fields for postId: {}", postId, ex);
                    }
                });
            } catch (JsonProcessingException e) {
                log.error("Failed to parse JSON from Kafka message: {}", textData.value(), e);
            }
        }
    }

}
 