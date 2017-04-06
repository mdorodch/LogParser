import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser {
	
	private static final String INPUT_FILENAME = "/home/itcs4102/Projects/srcJava/input/log.txt";
	private static final String OUTPUT_PATH = "/home/itcs4102/Projects/srcJava/output/";

	private static final String PATTERN = "([0-9.A-Za-z]+).*\\[(.*)\\][ ]+\"(.*)\"[ ]+([0-9]+)[ ]+([0-9]+)";



	public static void main(String[] args) throws NumberFormatException, IOException {
		// TODO Auto-generated method stub		

		Scanner sc=new Scanner(System.in);
		System.out.print("Enter how many number of top records you wish to retrieve ");
		int top=sc.nextInt();
		Map<String,HostLogData> logMap=new HashMap<String,HostLogData>();
		BufferedReader br = new BufferedReader(new FileReader(INPUT_FILENAME));

		String currentLine=null;
		String hostName=null;
		String time=null;
		String request=null;
		String status;
		int responseTime;

		Pattern pattern=Pattern.compile(PATTERN);
		LogDetails logDetails=null;
		HostLogData hostLogData=null;
		List<LogDetails> logDetailsList=null;
		long totalResponseTimes=0;
		long totalRequests=0;

		while ((currentLine = br.readLine()) != null) {
			//System.out.println(currentLine);
			Matcher m=pattern.matcher(currentLine);
			while(m.find()){
				hostName=m.group(1);
				time=m.group(2);
				request=m.group(3);
				status=m.group(4);
				responseTime=Integer.valueOf(m.group(5));

				/*System.out.println("Complete value: " + m.group(0) );
			        System.out.println("Host value: " + m.group(1) );
			        System.out.println("time value: " + m.group(2) );
			        System.out.println("request value: " + m.group(3) );
			        System.out.println("status value: " + m.group(4) );
			        System.out.println("response value: " + m.group(5) );*/

				if(logMap.containsKey(hostName)){
					hostLogData=logMap.get(hostName);
					logDetailsList=hostLogData.getLogDetails();
					logDetails=new LogDetails();
					logDetails.setRequest(request);
					logDetails.setResponseTime(responseTime);
					logDetails.setStatus(status);
					logDetails.setTime(time);
					logDetailsList.add(logDetails);
					hostLogData.setLogDetails(logDetailsList);

					//total response time
					totalResponseTimes=hostLogData.getTotalResponseTimes();
					totalResponseTimes+=responseTime;
					hostLogData.setTotalResponseTimes(totalResponseTimes);

					//total requests
					totalRequests=hostLogData.getTotalNumberOfRequest()+1;
					hostLogData.setTotalNumberOfRequest(totalRequests);

					//total success & failure response
					if(status.startsWith("4") || status.startsWith("5")){
						hostLogData.setFailureRequest(hostLogData.getFailureRequest()+1);
					}else{
						hostLogData.setSuccessRequest(hostLogData.getSuccessRequest()+1);
					}
					
					logMap.put(hostName, hostLogData);
				}else{
					hostLogData=new HostLogData();
					logDetailsList=new ArrayList<LogDetails>();
					logDetails=new LogDetails();
					logDetails.setRequest(request);
					logDetails.setResponseTime(responseTime);
					logDetails.setStatus(status);
					logDetails.setTime(time);


					logDetailsList.add(logDetails);

					//initial values
					hostLogData.setTotalNumberOfRequest(1);
					//intial response time 
					hostLogData.setTotalResponseTimes(responseTime);
					
					//counting success and failure response
					if(status.startsWith("4") || status.startsWith("5")){
						hostLogData.setFailureRequest(1);
						hostLogData.setSuccessRequest(0);
					}else{
						hostLogData.setFailureRequest(0);
						hostLogData.setSuccessRequest(1);
					}

					hostLogData.setLogDetails(logDetailsList);
					logMap.put(hostName, hostLogData);
				}
			}
		}

		Map<String,HostLogData> sortedByRequestMap=sortByTotalRequests(logMap);
		writeToFile(sortedByRequestMap,"toprequestsHost.txt",top);

		Map<String,HostLogData> sortedByResponseTimeMap=sortByTotalResponseTime(logMap);
		writeToFile(sortedByResponseTimeMap,"MaximumrequesttimeHost.txt",top);
		
		Map<String,HostLogData> sortedBySuccessRequestMap=sortBySuccessRequest(logMap);
		writeToFile(sortedBySuccessRequestMap,"TopSuccessRequestHost.txt",top);
		
		Map<String,HostLogData> sortedByFailureRequestMap=sortByFailureRequest(logMap);
		writeToFile(sortedByFailureRequestMap,"TopFailureRequestHost.txt",top);

	}
	private static Map<String, HostLogData> sortBySuccessRequest(Map<String, HostLogData> logMap) {
		List<Map.Entry<String,HostLogData>> entries = new ArrayList<Map.Entry<String,HostLogData>>(logMap.entrySet());
		Collections.sort(entries, new TotalSuccessRequestComparator());

		Map<String, HostLogData> sortedMap = new LinkedHashMap<String, HostLogData>();
		for (Map.Entry<String, HostLogData> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
	private static Map<String, HostLogData> sortByFailureRequest(Map<String, HostLogData> logMap) {
		List<Map.Entry<String,HostLogData>> entries = new ArrayList<Map.Entry<String,HostLogData>>(logMap.entrySet());
		Collections.sort(entries, new TotalFailureRequestComparator());

		Map<String, HostLogData> sortedMap = new LinkedHashMap<String, HostLogData>();
		for (Map.Entry<String, HostLogData> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}


	private static Map<String, HostLogData> sortByTotalRequests(Map<String, HostLogData> logMap) {
		List<Map.Entry<String,HostLogData>> entries = new ArrayList<Map.Entry<String,HostLogData>>(logMap.entrySet());
		Collections.sort(entries, new TotalRequestsComparator());

		Map<String, HostLogData> sortedMap = new LinkedHashMap<String, HostLogData>();
		for (Map.Entry<String, HostLogData> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
	private static Map<String, HostLogData> sortByTotalResponseTime(Map<String, HostLogData> logMap) {
		List<Map.Entry<String,HostLogData>> entries = new ArrayList<Map.Entry<String,HostLogData>>(logMap.entrySet());
		Collections.sort(entries, new TotalResponseTimeCompartor());

		Map<String, HostLogData> sortedMap = new LinkedHashMap<String, HostLogData>();
		for (Map.Entry<String, HostLogData> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	public static void writeToFile(Map<String, HostLogData> sortedByRequestMap,String fileName,int top) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(OUTPUT_PATH+fileName));
		//Collections.sort(logMap,new TotalRequestsComparator());
		int i=0;
		for (Map.Entry<String, HostLogData> entry : sortedByRequestMap.entrySet()) {
			if(i>=top){
				break;
			}
			i++;
			String key = entry.getKey();
			HostLogData value = entry.getValue();
			
			bw.write(key);
			bw.write("\t");
			bw.write("Number of Requests "+String.valueOf(value.getTotalNumberOfRequest()));
			bw.write("\t");
			bw.write("Response Time "+String.valueOf(value.getTotalResponseTimes()));
			bw.write("\t");
			bw.write("Total Success Request "+String.valueOf(value.getSuccessRequest()));
			bw.write("\t");
			bw.write("Total Failure Request "+String.valueOf(value.getFailureRequest()));
			bw.write("\t");
			for(LogDetails logDet:value.getLogDetails()){
				bw.write(logDet.getRequest());
				bw.write("\t");
			}
			bw.newLine();
		}
		bw.close();
	}
}
