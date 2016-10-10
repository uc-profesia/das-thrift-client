package com.profesia.wso2.das.thrift.clients;

import com.profesia.wso2.das.thirft.clients.DASManager;

import static org.junit.Assert.*;

import org.junit.Test;
import org.wso2.carbon.databridge.commons.utils.DataBridgeCommonsUtils;

public class DASManager_ {
	
	@Test
	public void _getURL() {
		DASManager cfg = new DASManager();
		cfg.init();
		assertEquals(cfg.getProperty(DASManager.DAS_THIRFT_URL),"tcp://localhost:7616");		
	}

	@Test
	public void _getProperty() {
		DASManager cfg = new DASManager();
		cfg.init();
		assertEquals(cfg.getProperty(DASManager.DAS_AGENTS_CONF),"/software/wso2das/repository/conf/data-bridge/data-agent-config.xml");		
	}
	
	@Test
	public void _getEventStreamId() {
		DASManager cfg = new DASManager();
		cfg.init();
		 assertEquals(DataBridgeCommonsUtils.generateStreamId(DASManager.DAS_EVENT_STREAM_NAME, DASManager.DAS_EVENT_STREAM_VERSION), DASManager.DAS_EVENT_STREAM_NAME+":"+DASManager.DAS_EVENT_STREAM_VERSION);
	}

}
