package com.vatechie.influx.api.utils;

import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.PropertySource;

import okhttp3.OkHttpClient;

@PropertySource(value = "classpath:influx.properties", ignoreResourceNotFound=true)
public class InfluxConnection {
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getName());
	
	public static InfluxDB influxConnection = null;
	public static Properties prop = ReadProperties.getInstancce();
	public static long networkConnectTimeout = 30000l;
	public static long networkReadTimeout = 60000l;
	public static long networkWriteTimeout = 30000l;
	public static long influxUnresponsiveSleepTime = 100L;
	
	public static InfluxDB getInstance(){
		if(influxConnection == null){
			try {
				influxConnection = createInfluxConnection();
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e.getMessage(),e);
			}
		}
		return influxConnection;
	}
	
	public InfluxConnection(){
		if(influxConnection == null){
			influxConnection = createInfluxConnection();
		}
	}
	
	public static InfluxDB createInfluxConnection(){
		
		try {
			networkConnectTimeout = Integer.parseInt(prop.getProperty("influx.db.network.connect.timeout"));
			networkReadTimeout = Integer.parseInt(prop.getProperty("influx.db.network.read.timeout"));
			networkWriteTimeout = Integer.parseInt(prop.getProperty("influx.db.network.write.timeout"));
		} catch (NumberFormatException e) {
			logger.error(e.getMessage(),e);
		}
		
		OkHttpClient.Builder okHttpClient = null;
		try {
			okHttpClient = new OkHttpClient().newBuilder();
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
		
		okHttpClient.connectTimeout(networkConnectTimeout, TimeUnit.MILLISECONDS);
		okHttpClient.readTimeout(networkReadTimeout, TimeUnit.MILLISECONDS);
		okHttpClient.writeTimeout(networkWriteTimeout, TimeUnit.MILLISECONDS);
		
		influxConnection = InfluxDBFactory.connect(prop.getProperty("influx.db.url"),
				prop.getProperty("influx.db.uid"), prop.getProperty("influx.db.pwd"),okHttpClient);
		
		if(checkInfluxConnection()){
			return influxConnection;
		}
		return null;
	}//method createInfluxConnection
	
	private static boolean checkInfluxConnection(){
		
		boolean influxDBstarted = false;
		do {
			Pong response;
			try {
				response = influxConnection.ping();
				logger.info("InfluxDB response : "+response);
				influxDBstarted = response.isGood();//true if the version of influxdb is not unknown.
			} catch (Exception e) {
				// NOOP intentional
				logger.error(e.getMessage(),e);
			}
			try {
				try {
					influxUnresponsiveSleepTime = Long.parseLong(prop.getProperty("influx.db.unrespnsive.sleep"));
				} catch (NumberFormatException e) {
					logger.error(e.getMessage(),e);
				}
				Thread.sleep(influxUnresponsiveSleepTime);
			} catch (InterruptedException e) {
				logger.error(e.getMessage(),e);
			}
		} while (!influxDBstarted);
		influxConnection.setLogLevel(LogLevel.NONE);
		
		logger.info("################################################################################## ");
		logger.info("#  Connected to InfluxDB Version: " + influxConnection.version() + " #");
		logger.info("##################################################################################");
		
		return influxDBstarted;
		
	}

}
