package com.vatechie.influx.api.dao;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.invoke.MethodHandles;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

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

@Repository
public class InfluxAPIRepository {
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getName());
	public static Properties prop = ReadProperties.getInstancce();
	private static InfluxDB influxDB;
	
	/*
	 * Insert all the points as a BatchPoints into the influxDB
	 * */
	public static void insertBatchPoints(BatchPoints batchPoints){

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
			logger.info("InfluxDB response : "+response);
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
		}
		
		logger.info("Successful insertion of data into the influxDB");
		
	}//method insertQueryData
	
	/*
	 * Influx data write method
	 * */
	public void insertCSVFileData(String fileName) {
		logger.info("******** Start of method insertCSVFileData ********");
		
		influxDB = InfluxConnection.getInstance();
		String dbName = prop.getProperty("influx.db.name");
		String dbRetentionPolicy = prop.getProperty("influx.db.retention.policy");
		String measurementName = prop.getProperty("influx.db.measurement.name");
		
		BatchPoints batchPoints = BatchPoints.database(dbName).retentionPolicy(dbRetentionPolicy)
				.consistency(InfluxDB.ConsistencyLevel.ALL).build();
		logger.info("BatchPoint created successfully");

		Point point = null;
		long timeToSet = 0l;
		
		try(BufferedReader br = new BufferedReader(new FileReader(fileName))) {
			String data;
			while((data = br.readLine()) != null) {
				logger.debug(data);
				String [] dataArray = data.split(",");
				//timeToSet = TimeConvertor.dateConvertToUTC();
				point = Point.measurement(measurementName).time(new Date().getTime(), TimeUnit.MILLISECONDS)
						.tag("namefirst", dataArray[0])
						.addField("namelast", dataArray[1])
						.addField("email", dataArray[2])
						.tag("username", dataArray[3])
						.addField("age", dataArray[4])
						.addField("country", dataArray[5]).build();
				batchPoints.point(point);
			}
		}catch(Exception e) {
			logger.error(e.getMessage());
		}
		
		insertBatchPoints(batchPoints);
		logger.info("******** End of method insertBatchPoints ********");
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
		
		for(QueryResult.Result result : qrObj.getResults()){
			if(result != null && result.getSeries()!= null){
				for(QueryResult.Series series : result.getSeries()){
					if(series != null){
						logger.trace("series.getName() = " + series.getName());
						logger.trace("series.getColumns() = "	+ series.getColumns());
						logger.trace("series.getValues() = " + series.getValues());
						logger.trace("series.getTags() = " + series.getTags());
						if(series.getValues() != null){
							logger.info("Deleting today's data from the influx");
							Query delQueryObj = new Query(influxDataDeleteQuery, dbName);
							influxDB.query(delQueryObj);
						}
					}
				}//inner for loop
			}else {
				logger.info("Nothing to delete from influx measurement.");
			}
		}//outer for loop
		logger.trace("End of checkAndDeleteCurrentDateDataEntries method.");
	}//checkAndDeleteCurrentDateDataEntries
	
}//class InfluxAPIRepository
