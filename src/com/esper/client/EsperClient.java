package com.esper.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.json.JSONObject;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.util.JSONEventRenderer;

public class EsperClient {
	
	private EPServiceProvider engine;
	
	public EsperClient() {
		this.engine = EPServiceProviderManager.getDefaultProvider();
	}
	
	public String getEngineURI() {
		return engine.getURI();
	}
	
	public void addEventType(String eventType, Map<String, Object> def) {
		engine.getEPAdministrator().getConfiguration().addEventType(eventType, def);
	}
	
	public EPStatement createStmt(String epl) {
		EPStatement stmt = engine.getEPAdministrator().createEPL(epl);
		return stmt;
	}
	
	public void addDataListener(String eventType, EPStatement stmt) {
		stmt.addListener(new DataListener(eventType, stmt));
	}

	public void sendEvent(Map<String, Object> event, String type) {
		engine.getEPRuntime().sendEvent(event, type);
	}
	
	public void read() {
		
		Random random = new Random();
		
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/disk3G.txt"));
			while(br.readLine()!=null){
				String s = br.readLine();
				String[] str;
				try{
					str = s.split("\t");
				}catch(NullPointerException e){
					continue;
				}
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS+00:00");
				Date date = null;
				try{
					date = simpleDateFormat.parse(str[0]);
				}catch(ParseException e){
					continue;
				}
				long ts = date.getTime();
				Map<String, Object> event = new HashMap<String, Object>();
				event.put("ts", ts);
				event.put("index", str[1]);
				event.put("bm05", random.nextInt(2));
				event.put("bm06", random.nextInt(2));
				event.put("bm07", random.nextInt(2));
				event.put("bm08", random.nextInt(2));
				event.put("bm09", random.nextInt(2));
				event.put("bm10", random.nextInt(2));
				//System.out.println(new JSONObject(event).toString());
				sendEvent(event, "DataPoint");
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} /*catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
	}
	
	private class DataListener implements UpdateListener {
		
		private String eventType;
		private EPStatement stmt;
		
		public DataListener(String eventType, EPStatement stmt) {
			this.eventType = eventType;
			this.stmt = stmt;
		}

		@Override
		public void update(EventBean[] newEvents, EventBean[] oldEvents) {
			// TODO Auto-generated method stub
			
			JSONEventRenderer jsonEventRenderer = engine.getEPRuntime().
					getEventRenderer().getJSONRenderer(stmt.getEventType());
			String json = jsonEventRenderer.render(newEvents[0]);
			
			JSONObject jsonObj = new JSONObject(json);
			System.out.println(eventType + ":" + jsonObj.toString());
			Map<String, Object> event = jsonObj.toMap();
			
			sendEvent(event, eventType);
			
		}
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Date start = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		System.out.println("start time:" + dateFormat.format(start));
		
		EsperClient ec = new EsperClient();
		
		Map<String, Object> dataDef0 = new HashMap<String, Object>();
		dataDef0.put("ts", int.class);
		dataDef0.put("index", String.class);
		dataDef0.put("mf01", String.class);
		dataDef0.put("mf02", String.class);
		dataDef0.put("mf03", String.class);
		dataDef0.put("pc13", String.class);
		dataDef0.put("pc14", String.class);
		dataDef0.put("pc15", String.class);
		dataDef0.put("pc25", String.class);
		dataDef0.put("pc26", String.class);
		dataDef0.put("pc27", String.class);
		dataDef0.put("bm05", int.class);
		dataDef0.put("bm06", int.class);
		dataDef0.put("bm07", int.class);
		dataDef0.put("bm08", int.class);
		dataDef0.put("bm09", int.class);
		dataDef0.put("bm10", int.class);
		
		Map<String, Object> dataDef = new HashMap<String, Object>();
		dataDef.put("ts", int.class);
		dataDef.put("index", String.class);
		dataDef.put("bm05", int.class);
		dataDef.put("bm06", int.class);
		dataDef.put("bm07", int.class);
		dataDef.put("bm08", int.class);
		dataDef.put("bm09", int.class);
		dataDef.put("bm10", int.class);
		
		Map<String, Object> data05Def = new HashMap<String, Object>();
		data05Def.put("ts", int.class);
		data05Def.put("index", String.class);
		data05Def.put("bm05", int.class);
		data05Def.put("sumbm05", int.class);
		
		Map<String, Object> data08Def = new HashMap<String, Object>();
		data08Def.put("ts", int.class);
		data08Def.put("index", String.class);
		data08Def.put("bm08", int.class);
		data08Def.put("sumbm08", int.class);
		
		Map<String, Object> data58Def = new HashMap<String, Object>();
		data58Def.put("ts58", int.class);
		
		Map<String, Object> data58AlarmDef = new HashMap<String, Object>();
		data58AlarmDef.put("alarm", int.class);
		data58AlarmDef.put("ts58", int.class);
		
		Map<String, Object> data06Def = new HashMap<String, Object>();
		data06Def.put("ts", int.class);
		data06Def.put("index", String.class);
		data06Def.put("bm06", int.class);
		data06Def.put("sumbm06", int.class);
		
		Map<String, Object> data09Def = new HashMap<String, Object>();
		data09Def.put("ts", int.class);
		data09Def.put("index", String.class);
		data09Def.put("bm09", int.class);
		data09Def.put("sumbm09", int.class);
		
		Map<String, Object> data69Def = new HashMap<String, Object>();
		data69Def.put("ts69", int.class);
		
		Map<String, Object> data69AlarmDef = new HashMap<String, Object>();
		data69AlarmDef.put("alarm", int.class);
		data69AlarmDef.put("ts69", int.class);
		
		Map<String, Object> data07Def = new HashMap<String, Object>();
		data07Def.put("ts", int.class);
		data07Def.put("index", String.class);
		data07Def.put("bm07", int.class);
		data07Def.put("sumbm07", int.class);
		
		Map<String, Object> data10Def = new HashMap<String, Object>();
		data10Def.put("ts", int.class);
		data10Def.put("index", String.class);
		data10Def.put("bm10", int.class);
		data10Def.put("sumbm10", int.class);
		
		Map<String, Object> data710Def = new HashMap<String, Object>();
		data710Def.put("ts710", int.class);
		
		Map<String, Object> data710AlarmDef = new HashMap<String, Object>();
		data710AlarmDef.put("alarm", int.class);
		data710AlarmDef.put("ts710", int.class);
		
		ec.addEventType("DataPoint", dataDef0);
		ec.addEventType("CDataPoint", dataDef);
		
		ec.addEventType("C05DataPoint", data05Def);
		ec.addEventType("C08DataPoint", data08Def);
		ec.addEventType("C58DataPoint", data58Def);
		ec.addEventType("Alarm58DataPoint", data58AlarmDef);
		
		ec.addEventType("C06DataPoint", data06Def);
		ec.addEventType("C09DataPoint", data09Def);
		ec.addEventType("C69DataPoint", data69Def);
		ec.addEventType("Alarm69DataPoint", data69AlarmDef);
		
		ec.addEventType("C07DataPoint", data07Def);
		ec.addEventType("C10DataPoint", data10Def);
		ec.addEventType("C710DataPoint", data710Def);
		ec.addEventType("Alarm710DataPoint", data710AlarmDef);
		
		String epl0 = "select ts,index,bm05,bm06,bm07,bm08,bm09,bm10 from DataPoint";
		String epl1 = "select ts,index,bm05,sum(bm05) as sumbm05 from CDataPoint#length(2) having sum(bm05)=1";
		String epl2 = "select ts,index,bm08,sum(bm08) as sumbm08 from CDataPoint#length(2) having sum(bm08)=1";
		String epl3 = "select (C05DataPoint.ts-C08DataPoint.ts) as ts58 from C05DataPoint#lastevent,C08DataPoint#lastevent";
		String epl4 = "insert into Alarm58DataPoint (alarm,ts58) select 1,ts58 from C58DataPoint where ts58>1.01*(select ts58 from C58DataPoint#length(2)).firstOf()";
		
		String epl5 = "select ts,index,bm06,sum(bm06) as sumbm06 from CDataPoint#length(2) having sum(bm06)=1";
		String epl6 = "select ts,index,bm09,sum(bm09) as sumbm09 from CDataPoint#length(2) having sum(bm09)=1";
		String epl7 = "select (C06DataPoint.ts-C09DataPoint.ts) as ts69 from C06DataPoint#lastevent,C09DataPoint#lastevent";
		String epl8 = "insert into Alarm69DataPoint (alarm,ts69) select 1,ts69 from C69DataPoint where ts69>1.01*(select ts69 from C69DataPoint#length(2)).firstOf()";
		
		String epl9 = "select ts,index,bm07,sum(bm07) as sumbm07 from CDataPoint#length(2) having sum(bm07)=1";
		String epl10 = "select ts,index,bm10,sum(bm10) as sumbm10 from CDataPoint#length(2) having sum(bm10)=1";
		String epl11 = "select (C07DataPoint.ts-C10DataPoint.ts) as ts710 from C07DataPoint#lastevent,C10DataPoint#lastevent";
		String epl12 = "insert into Alarm710DataPoint (alarm,ts710) select 1,ts710 from C710DataPoint where ts710>1.01*(select ts710 from C710DataPoint#length(2)).firstOf()";
		
		ec.addDataListener("CDataPoint", ec.createStmt(epl0));
		
		ec.addDataListener("C05DataPoint", ec.createStmt(epl1));
		ec.addDataListener("C08DataPoint", ec.createStmt(epl2));
		ec.addDataListener("C58DataPoint", ec.createStmt(epl3));
		ec.addDataListener("Alarm58DataPoint", ec.createStmt(epl4));
		
		ec.addDataListener("C06DataPoint", ec.createStmt(epl5));
		ec.addDataListener("C09DataPoint", ec.createStmt(epl6));
		ec.addDataListener("C69DataPoint", ec.createStmt(epl7));
		ec.addDataListener("Alarm69DataPoint", ec.createStmt(epl8));
		
		ec.addDataListener("C07DataPoint", ec.createStmt(epl9));
		ec.addDataListener("C10DataPoint", ec.createStmt(epl10));
		ec.addDataListener("C710DataPoint", ec.createStmt(epl11));
		ec.addDataListener("Alarm710DataPoint", ec.createStmt(epl12));
		
		/*Map<String, Object> event1 = new HashMap<String, Object>();
		event1.put("ts", 1506495542);
		event1.put("index", "2556001");
		event1.put("bm05", 0);
		event1.put("bm06", 0);
		event1.put("bm07", 0);
		event1.put("bm08", 1);
		event1.put("bm09", 0);
		event1.put("bm10", 0);
		
		Map<String, Object> event2 = new HashMap<String, Object>();
		event2.put("ts", 1506495543);
		event2.put("index", "2556002");
		event2.put("bm05", 1);
		event2.put("bm06", 0);
		event2.put("bm07", 0);
		event2.put("bm08", 0);
		event2.put("bm09", 0);
		event2.put("bm10", 0);
		
		Map<String, Object> event3 = new HashMap<String, Object>();
		event3.put("ts", 1506495545);
		event3.put("index", "2556003");
		event3.put("bm05", 0);
		event3.put("bm06", 0);
		event3.put("bm07", 0);
		event3.put("bm08", 1);
		event3.put("bm09", 0);
		event3.put("bm10", 0);
		
		Map<String, Object> event4 = new HashMap<String, Object>();
		event4.put("ts", 1506495546);
		event4.put("index", "2556004");
		event4.put("bm05", 1);
		event4.put("bm06", 0);
		event4.put("bm07", 0);
		event4.put("bm08", 0);
		event4.put("bm09", 0);
		event4.put("bm10", 0);
		
		ec.sendEvent(event1, "CDataPoint");
		ec.sendEvent(event2, "CDataPoint");
		ec.sendEvent(event3, "CDataPoint");
		ec.sendEvent(event4, "CDataPoint");*/
		
		ec.read();
		
		Date end = new Date();
		System.out.println("end time:" + dateFormat.format(end));
		
		/*try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

}
