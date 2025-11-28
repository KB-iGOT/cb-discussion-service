package com.igot.cb.pores.util;

import org.igot.common.ApiResponse;
import org.springframework.http.HttpStatus;

public class ProjectUtil {

  public static ApiResponse returnErrorMsg(String error, HttpStatus type, ApiResponse response, String status) {
    response.setResponseCode(type);
    response.getParams().setErr(error);
    response.getParams().setStatus(status);
    return response;
  }

}
