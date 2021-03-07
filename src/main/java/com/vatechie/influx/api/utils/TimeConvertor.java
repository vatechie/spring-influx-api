package com.vatechie.influx.api.utils;

import java.lang.invoke.MethodHandles;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeConvertor {
	private static final Logger slf4jLogger = LoggerFactory
			.getLogger(MethodHandles.lookup().lookupClass().getName());

	public static long dateConvertToUTC(String date) {

		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S",Locale.ENGLISH);
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date2 = null;
		try {
			date2 = format.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
			slf4jLogger.error(e.getMessage(),e);
		}
		return date2.getTime();
	}//method dateConvertToUTC
	
	public static long convertUTCTimeToLong(String date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);
			format.setTimeZone(TimeZone.getTimeZone("UTC"));
		
		Date date2 = null;
		try {
			date2 = format.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
			slf4jLogger.error(e.getMessage(),e);
		}
		return date2.getTime();
	}
	
	public static long convertUTCTimeWithMSToLong(String date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
			format.setTimeZone(TimeZone.getTimeZone("UTC"));
		
		Date date2 = null;
		try {
			date2 = format.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
			slf4jLogger.error(e.getMessage(),e);
		}
		return date2.getTime();
	}
	
	public static String dateConvertToUTC(Date date) {

		/*DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S",Locale.ENGLISH);
		Date date2 = null;
		try {
			date2 = format.parse(date.toString());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date2.toString();*/
		
		DateFormat formatterIST = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
		formatterIST.setTimeZone(TimeZone.getTimeZone("Asia/Kolkata")); // better than using IST
		Date ISTDate = null;
		try {
			ISTDate = formatterIST.parse(date.toString());
		} catch (ParseException e) {
			e.printStackTrace();
			slf4jLogger.error(e.getMessage(),e);
		}
		
		DateFormat formatterUTC = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
		formatterUTC.setTimeZone(TimeZone.getTimeZone("UTC"));
		
		return formatterUTC.format(ISTDate);
		
	}//method dateConvertToUTC
	
	public static long dateConvertToUTCLong(String date) {
				
		DateFormat formatterUTC = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
		formatterUTC.setTimeZone(TimeZone.getTimeZone("UTC"));
		
		long UTCDate = 0;
		try {
			UTCDate = formatterUTC.parse(date).getTime();
			
		} catch (ParseException e) {
			e.printStackTrace();
			slf4jLogger.error(e.getMessage(),e);
		}
		return UTCDate;
		
	}//method dateConvertToUTCLong
	
}//class TimeConvertor
