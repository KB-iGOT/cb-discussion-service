package com.igot.cb.metrics;

import com.igot.cb.pores.util.Constants;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class DBStatsComponent {

    private int operationCount = 0;
    private long totalTimeTaken = 0;
    private long operationStartTime = 0;

    public void startOperation() {
        if (operationStartTime != 0) {
            throw new IllegalStateException("An operation is already in progress.");
        }
        operationStartTime = System.currentTimeMillis();
    }

    public void endOperation() {
        if (operationStartTime == 0) {
            throw new IllegalStateException("No operation is currently in progress.");
        }
        long elapsedTime = System.currentTimeMillis() - operationStartTime;
        totalTimeTaken += elapsedTime;
        operationCount++;
        operationStartTime = 0;
    }

    public Map<String, Object> getOperationStats() {
        Map<String, Object> operationData = new HashMap<>();
        operationData.put(Constants.OPERATION_COUNT, operationCount);
        operationData.put(Constants.TIME_TAKEN, totalTimeTaken);
        operationCount = 0;
        totalTimeTaken = 0;
        return operationData;
    }
}
