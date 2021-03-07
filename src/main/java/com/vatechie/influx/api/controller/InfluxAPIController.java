package com.vatechie.influx.api.controller;

import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.vatechie.influx.api.service.InfluxAPIService;

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
	
	@PostMapping("/insert/filedata")
	public String insertCSVData() {
		logger.info("Influx insert file data API called...");
		return influxService.insertData();
	}
	
	@PostMapping("/insert/logs")
	public String insertLogData() {
		logger.info("Influx insert log API called...");
		return influxService.insertData();
	}
	
	@PostMapping("/insert/query/data")
	public String insertQueryData() {
		logger.info("Influx insert query data API called...");
		return influxService.insertData();
	}

}
