package com.vatechie.influx.api.service;

import java.lang.invoke.MethodHandles;

import com.vatechie.influx.api.dto.DataIngestionResponse;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Pong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.vatechie.influx.api.repository.InfluxAPIRepository;
import com.vatechie.influx.api.utils.InfluxConnection;
import org.springframework.web.multipart.MultipartFile;

@Service
public class InfluxAPIService {
	
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getName());
	
	InfluxAPIRepository influxRepo;
	
	InfluxAPIService(InfluxAPIRepository influxRepo){
		this.influxRepo = influxRepo;
	}
	
	public Boolean checkInfluxStatus() {
		logger.info("Checking influx connection");
		InfluxDB influxDB = InfluxConnection.getInstance();
		
		Pong response = null;
		try {
			response = influxDB.ping();
		} catch (Exception e) {
			// NOOP intentional
			logger.error(e.getMessage(),e);
		}
		return response.isGood();
	}

	public DataIngestionResponse insertData(MultipartFile file) {
		logger.info("Insert api service called...");
		return influxRepo.insertCSVFileData(file);
	}
}
