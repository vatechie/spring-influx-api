package com.vatechie.influx.api.utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.invoke.MethodHandles;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadProperties {
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getName());
	
	static Properties properties = null;
	FileReader reader ;
	
	public ReadProperties(){}

	public static Properties getInstancce(){
		if ( properties==null){
			properties= new ReadProperties().loadProperties();
		}
		return properties;
	}
	
	public static String getProperty(String propKey){
		return properties.getProperty(propKey);
	}
	
	public static Properties getInstancce(String propName){
		if ( properties==null){
			properties= new ReadProperties().loadProperties(propName);
		}
		return properties;
	}
	
	Properties loadProperties(){
		Properties prop = new Properties();
		String propName = "influx.properties";
		
		try{
			if(System.getProperty("os.name").startsWith("Windows")){
				prop.load(getClass().getClassLoader().getResourceAsStream(propName));
				logger.trace("Property has been loaded in properties static object");
			}	
			else{
				try {
					reader = new FileReader(propName);
				} catch (FileNotFoundException e) {
					logger.error(e.getMessage(),e);
				}
				if(reader!=null){
					prop.load(reader);
					logger.trace("Property has been loaded in properties static object");
				}
			}
		}catch(Exception e){
			logger.info("Error occured while loading the property file");
			logger.error(e.getMessage(),e);
		}
		
		return prop;	
	}
	
	Properties loadProperties(String propName){
		Properties prop = new Properties();
		
		try{
			if(System.getProperty("os.name").startsWith("Windows")){
				prop.load(getClass().getClassLoader().getResourceAsStream(propName));
			}	
			else{
				try {
					reader = new FileReader(propName);
				} catch (FileNotFoundException e) {
					logger.error(e.getMessage(),e);
				}
				if(reader!=null){
					prop.load(reader);
				}
			}
		}catch(Exception e){
			logger.error(e.getMessage(),e);
		}
		
		return prop;	
	}
}