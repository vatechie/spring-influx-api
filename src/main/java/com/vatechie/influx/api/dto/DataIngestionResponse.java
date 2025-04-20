package com.vatechie.influx.api.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DataIngestionResponse {
    private String message;
    private Boolean status;
}
