package com.igot.cb.profanity.impl;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.pores.util.CbServerProperties;
import com.igot.cb.pores.util.Constants;
import com.igot.cb.profanity.IProfanityCheckService;
import com.igot.cb.transactional.service.RequestHandlerServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the IProfanityCheckService interface that processes profanity checks
 * for discussion details by sending a request to a text moderation API.
 */
@Service
public class ProfanityCheckServiceImpl implements IProfanityCheckService {
    @Autowired
    private CbServerProperties cbServerProperties;

    @Autowired
    private RequestHandlerServiceImpl requestHandlerService;

    /**
     * Processes a profanity check for a discussion by sending the discussion details
     * to a text moderation API.
     *
     * @param id the ID of the discussion to check
     * @param discussionDetailsNode the details of the discussion as an ObjectNode
     */
    @Override
    public void processProfanityCheck(String id, ObjectNode discussionDetailsNode) {
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
        headerMap.put(Constants.AUTHORIZATION, cbServerProperties.getCbDiscussionApiKey());
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(Constants.POST_ID, id);
        metadata.put(Constants.TYPE, discussionDetailsNode.get(Constants.TYPE).asText());
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.TEXT, discussionDetailsNode.get(Constants.DESCRIPTION).asText());
        requestBody.put(Constants.LANGUAGE, discussionDetailsNode.get(Constants.LANGUAGE).asText());
        requestBody.put(Constants.METADATA, metadata);
        Map<String, Object> mainRequest = new HashMap<>();
        mainRequest.put(Constants.HEADER_MAP, headerMap);
        mainRequest.put(Constants.REQUEST_BODY, requestBody);
        mainRequest.put(Constants.SERVICE_CODE, Constants.PROFANITY_CHECK);
        requestHandlerService.fetchResultUsingPost(cbServerProperties.getCbServiceRegistryBaseUrl() + "/" + cbServerProperties.getCbRegistryTextModerationApiPath(), mainRequest, headerMap);
    }
}
