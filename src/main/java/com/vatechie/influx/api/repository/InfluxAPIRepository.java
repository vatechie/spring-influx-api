package com.vatechie.influx.api.repository;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.vatechie.influx.api.dto.DataIngestionResponse;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.vatechie.influx.api.utils.InfluxConnection;
import com.vatechie.influx.api.utils.ReadProperties;
import com.vatechie.influx.api.utils.TimeConvertor;
import org.springframework.web.multipart.MultipartFile;

@Repository
public class InfluxAPIRepository {
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getName());
	public static Properties prop = ReadProperties.getInstancce();
	private static InfluxDB influxDB;
	
	/*
	 * Insert all the points as a BatchPoints into the influxDB
	 * */
	public static Boolean insertBatchPoints(BatchPoints batchPoints){

		influxDB = InfluxConnection.getInstance();
		
		int sleepTime = 1000;
		try {
			sleepTime = Integer.parseInt(prop.getProperty("influx.db.batch.sleeptime"));
		} catch (NumberFormatException e1) {
			logger.error(e1.getMessage(),e1);
		}
		
		Pong response;
		response = influxDB.ping();
		try {
			response = influxDB.ping();
			logger.info("InfluxDB response : " + response);
			if (response.isGood()) {
				try {
					influxDB.write(batchPoints);
					Thread.sleep(sleepTime);
				} catch (Exception e) {
					logger.error(e.getMessage(),e);
				}
			}
		} catch (Exception e) {
			// NOOP intentional
			logger.error(e.getMessage(),e);
			return false;
		}
		
		logger.info("Successful insertion of data into the influxDB");
		return true;
	}
	
	/*
	 * Influx data write method
	 * */
	public DataIngestionResponse insertCSVFileData(MultipartFile file) {
		logger.info("******** Start of method insertCSVFileData ********");
		
		influxDB = InfluxConnection.getInstance();
		String dbName = prop.getProperty("influx.db.name");
		String dbRetentionPolicy = prop.getProperty("influx.db.retention.policy");
		String measurementName = prop.getProperty("influx.db.measurement.name");
		
		BatchPoints batchPoints = BatchPoints.database(dbName).retentionPolicy(dbRetentionPolicy)
				.consistency(InfluxDB.ConsistencyLevel.ALL).build();
		logger.info("BatchPoint created successfully");

		long timeToSet = 0l;
		
		try(BufferedReader br = new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) {

			String headerLine = br.readLine();
			if (headerLine == null) {
				return DataIngestionResponse.builder()
						.message( "CSV header is missing.")
						.status(false)
						.build();
			}

			String[] headers = headerLine.split(",");
			Map<String, Integer> headerMap = new HashMap<>();
			for (int i = 0; i < headers.length; i++) {
				headerMap.put(headers[i].trim().toLowerCase(), i);
			}

			String data;
			while((data = br.readLine()) != null) {
				logger.debug(data);
				String [] dataArray = data.split(",");

				//timeToSet = TimeConvertor.dateConvertToUTC();
				Point.Builder pointBuilder = Point.measurement(measurementName).time(new Date().getTime(), TimeUnit.MILLISECONDS);

				for (Map.Entry<String, Integer> entry : headerMap.entrySet()) {
					String key = entry.getKey();
					int index = entry.getValue();
					String value = index < dataArray.length ? dataArray[index].trim() : "";
					pointBuilder.addField(key, value);
					// Config can be provided to decide which header field data is stored as tag or field
					// pointBuilder.tag(key, value);
				}

				batchPoints.point(pointBuilder.build());
			}
		} catch(Exception e) {
			logger.error(e.getMessage());
		}
		
		Boolean status = insertBatchPoints(batchPoints);
		logger.info("******** End of method insertBatchPoints ********");
		return DataIngestionResponse.builder()
				.status(status)
				.build();
	}
	 
	public void checkAndDeleteCurrentDateDataEntries(){
		logger.trace("Start of checkAndDeleteCurrentDateDataEntries method:");
		influxDB = InfluxConnection.getInstance();
		
		String dbName = prop.getProperty("influx.db.name");
		String measurementName = prop.getProperty("influx.db.measurement.name");
		
		Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY,0);
        cal.set(Calendar.MINUTE,0);
        cal.set(Calendar.SECOND,0);
        cal.set(Calendar.MILLISECOND,0);
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        
        Date currentIntialDate = cal.getTime();
        
        long initialTime = TimeConvertor.dateConvertToUTCLong(currentIntialDate.toString());
		
		String influxQuery = "";
		String influxDataDeleteQuery = "";
		
		influxQuery =  "select * from " + measurementName + " where time >= "+initialTime;
		influxDataDeleteQuery = "delete from "+ measurementName + " where time >= "+initialTime;
		
		logger.info("Influx query to fetch the data from influx: "+influxQuery);
		
		Query qObj = new Query(influxQuery, dbName);
		QueryResult qrObj = influxDB.query(qObj);
		
		for (QueryResult.Result result : qrObj.getResults()) {
			if (result != null && result.getSeries()!= null) {
				for (QueryResult.Series series : result.getSeries()){
					if (series != null) {
						logger.trace("series.getName() = " + series.getName());
						logger.trace("series.getColumns() = "	+ series.getColumns());
						logger.trace("series.getValues() = " + series.getValues());
						logger.trace("series.getTags() = " + series.getTags());
						if (series.getValues() != null) {
							logger.info("Deleting today's data from the influx");
							Query delQueryObj = new Query(influxDataDeleteQuery, dbName);
							influxDB.query(delQueryObj);
						}
					}
				}
			} else {
				logger.info("Nothing to delete from influx measurement.");
			}
		}
		logger.trace("End of checkAndDeleteCurrentDateDataEntries method.");
	}
	
}
