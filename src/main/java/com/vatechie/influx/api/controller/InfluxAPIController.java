package com.vatechie.influx.api.controller;

import java.lang.invoke.MethodHandles;

import com.vatechie.influx.api.dto.DataIngestionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.vatechie.influx.api.service.InfluxAPIService;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/influx")
public class InfluxAPIController {
	
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getName());
	
	@Autowired
	InfluxAPIService influxService;
	
	@GetMapping("/status")
	public Boolean getStatus() {
		return influxService.checkInfluxStatus();
	}
	
	@PostMapping(value = "/insert/file-data", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
	public ResponseEntity<DataIngestionResponse> insertCSVData(@RequestPart("file") MultipartFile file) {

		if (file.isEmpty() || !file.getOriginalFilename().endsWith(".csv")) {
			return ResponseEntity.badRequest().body(DataIngestionResponse.builder()
					.message("Please upload a valid CSV file.")
					.build());
		}

		logger.debug("Insert file data into influx db");
		return ResponseEntity.ok(influxService.insertData(file));
	}

}
