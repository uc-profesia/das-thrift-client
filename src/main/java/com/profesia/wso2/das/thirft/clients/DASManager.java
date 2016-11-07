package com.profesia.wso2.das.thirft.clients;

import java.io.File;
import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;
import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAgentConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAuthenticationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointException;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.exception.TransportException;
import org.wso2.carbon.databridge.commons.utils.DataBridgeCommonsUtils;
import org.wso2.carbon.utils.CarbonUtils;

public class DASManager {
	
	final static Logger logger = Logger.getLogger(DASManager.class);

	private final String CFG_FILE = "das";
	private ResourceBundle labels;

	private DataPublisher dataPublisher;
	
	private String eventStreamId = "";
	
	public static final String DAS_THIRFT_URL = "das.thrift.url";
	public static final String DAS_USERNAME = "das.username";
	public static final String DAS_PASSWORD = "das.password";
	public static final String DAS_EVENT_STREAM_NAME = "das.event.stream.name";
	public static final String DAS_EVENT_STREAM_VERSION = "das.event.stream.version";	
	public static final String DAS_AGENTS_CONF = "das.agents.conf";	
	public static final String DAS_AGENTS_TYPE = "das.agents.type";

	private final String JAVAX_NET_SLL_TRUSTSTORE= "javax.net.ssl.trustStore";	
	private final String JAVAX_NET_SLL_TRUSTSTOREPASSWORD= "javax.net.ssl.trustStorePassword";
	
	private final String Security_KeyStore_Location= "javax.net.ssl.trustStore";	
	private final String Security_KeyStore_Password= "javax.net.ssl.trustStorePassword";

	public void init() {
		labels = ResourceBundle.getBundle(CFG_FILE);
		AgentHolder.setConfigPath(getProperty(DAS_AGENTS_CONF));
		
		System.setProperty("javax.net.ssl.trustStore", getProperty(JAVAX_NET_SLL_TRUSTSTORE));
		System.setProperty("javax.net.ssl.trustStorePassword", getProperty(JAVAX_NET_SLL_TRUSTSTOREPASSWORD));	
        System.setProperty("Security.KeyStore.Location", getProperty(Security_KeyStore_Location));
        System.setProperty("Security.KeyStore.Password", getProperty(Security_KeyStore_Password));

        
        this.eventStreamId = DataBridgeCommonsUtils.generateStreamId(getProperty(DAS_EVENT_STREAM_NAME), getProperty(DAS_EVENT_STREAM_VERSION));	
		try {
			dataPublisher = new DataPublisher(getProperty(DAS_THIRFT_URL), getProperty(DAS_USERNAME), getProperty(DAS_PASSWORD));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Error loading publisher: " + e.getMessage());
		}
	}

	public String getProperty(String property) {
		return labels.getString(property);
	}
	
	public String getEventStreamId() {
		return this.eventStreamId;
	}
	
	public void publish(Object[] obja) {
		try {
			Event event = new Event();
			event.setPayloadData(obja);
			event.setStreamId(this.getEventStreamId());
			event.setTimeStamp(System.currentTimeMillis());
			dataPublisher.tryPublish(event);
			logger.warn("This message has been sent: " + event.toString());
		} catch (Exception e) {
			logger.error("Event publishing failed: " + e.getMessage());
		}
		
	}
}
