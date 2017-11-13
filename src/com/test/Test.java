package com.test;

import java.util.HashMap;
import java.util.Map;

import com.esper.client.EsperClient;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		EsperClient ec = new EsperClient();
		Map<String, Object> def = new HashMap<String, Object>();
		def.put("age", int.class);
		def.put("name", String.class);
		ec.addEventType("person", def);
		
		String epl = "select * from person";
		ec.addDataListener("out", ec.createStmt(epl));
		
		Map<String, Object> event = new HashMap<String, Object>();
		event.put("test", "test");
		ec.sendEvent(event, "test");

	}

}
