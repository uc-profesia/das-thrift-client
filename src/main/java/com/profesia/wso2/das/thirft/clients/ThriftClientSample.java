package com.profesia.wso2.das.thirft.clients;

import java.util.ResourceBundle;

import org.slf4j.LoggerFactory;
import org.wso2.carbon.databridge.commons.Event;
import org.slf4j.Logger;



public class ThriftClientSample {
	final static Logger logger = (Logger) LoggerFactory.getLogger(ThriftClientSample.class);
	
	private static String CFG_FILE="client";
	private static ResourceBundle options;
	public static void main(String[] args) {
		options = ResourceBundle.getBundle(CFG_FILE);
		DASManager cfg = new DASManager();
		cfg.init();
		while (true) {
			long timestamp = System.currentTimeMillis();
			long random = Math.round(Math.ceil(Math.random()*1000));
			logger.info("Timestamp: " + timestamp + "; Random: " + random);
			cfg.publish(new Object[] {timestamp, random}); 
			try {
				Thread.sleep(Integer.parseInt(options.getString("options.sleep")));
			} catch (NumberFormatException e) {
				logger.error("Error generating random fields");
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
