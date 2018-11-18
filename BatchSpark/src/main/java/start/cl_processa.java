package start;

import static org.apache.spark.sql.functions.col;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class cl_processa {
	
	final static String gv_table = "JSON8";
	final static String gv_zkurl = "localhost:2181";
	
	final static String gc_conn = "CONN";
	final static String gc_dns  = "DNS";
	final static String gc_http = "HTTP";
	
	private static cl_processa gv_processa;

	private static Map<String, String> gv_phoenix;
	
	private static SparkConf gv_conf;
    
	private static SparkContext gv_context;
	                        
	private static SparkSession gv_session;
	
	public static void main(String[] args) throws AnalysisException {
	
		gv_processa = new cl_processa();
		
		gv_processa.m_start();
	}
	
	public void m_start() throws AnalysisException {
						
		m_conecta_phoenix();
		
		m_seleciona();
		
	}
	
	public static void m_conecta_phoenix(){
		
		gv_phoenix = new HashMap<String, String>();
		
		gv_phoenix.put("zkUrl", gv_zkurl);
		gv_phoenix.put("hbase.zookeeper.quorum", "master");
		gv_phoenix.put("table", gv_table);
		
		gv_conf = new SparkConf().setMaster("local[2]").setAppName("SelectLog");
		
		gv_context = new SparkContext(gv_conf);
		
		gv_session = new SparkSession(gv_context);		
		
		Logger.getRootLogger().setLevel(Level.ERROR);
	}
	
	public void m_seleciona() throws AnalysisException {
							
		Dataset<Row> lv_data = gv_session
							   .sqlContext()
							   .read()
							   .format("org.apache.phoenix.spark")
							   .options(gv_phoenix)
							   .load();
		
		//lv_data.createOrReplaceTempView(gv_table); //cria uma tabela temporaria, para acessar via SQL
		
		System.out.println("Conexões TOTAL: \t"+lv_data.count() + "\n\n");
		
		m_processa_conn(lv_data);
		//m_processa_dns(lv_data);
		//m_processa_http(lv_data);
		
	}
	
	public void m_processa_conn(Dataset<Row> lv_data) throws AnalysisException {
		
		Dataset<Row> lv_conn;	
		
		lv_conn = lv_data
				  .select("TIPO",              
						  "TS_CODE",  								   	
						  "TS",         
						  "UID",
						  "ID_ORIG_H",
						  "ID_ORIG_P",  
						  "ID_RESP_H",  
						  "ID_RESP_P",  
						  "PROTO",
						  "SERVICE",	
						  "DURATION",  
						  "ORIG_BYTES",
						  "RESP_BYTES",
						  "CONN_STATE",
						  "LOCAL_ORIG",
						  "LOCAL_RESP",
						  "missed_bytes",	
						  "history",		
						  "orig_pkts",		
						  "orig_ip_bytes",	
						  "resp_pkts",		
						  "resp_ip_bytes",  
						  "tunnel_parents"
						  )				  
				  .filter(col("TIPO").equalTo(gc_conn));
		
		/*		String lv_sql;
		 * lv_sql = "SELECT UID, TS_CODE FROM JSON6 WHERE TIPO = 'CONN' ";
		
		System.out.println("SQL: "+lv_sql);
		
		
		lv_conn = lv_data
				.sparkSession()
				.sql(lv_sql);*/
						
		lv_conn.printSchema();
		
		System.out.println("Conexões CONN: \t"+lv_conn.count());
		
		lv_conn.show(1000);
		
	}
	
	public void m_processa_dns(Dataset<Row> lv_data) {

		Dataset<Row> lv_dns;

		lv_dns = lv_data
				.select("TIPO",              
						"TS_CODE",  								   	
						"TS",         
						"UID",
						"ID_ORIG_H",
						"ID_ORIG_P",  
						"ID_RESP_H",  
						"ID_RESP_P",  
						"PROTO",
						"TRANS_ID",	
						"QUERY",  		
						"QCLASS", 		
						"QCLASS_NAME",
						"QTYPE", 		
						"QTYPE_NAME", 
						"RCODE",   	
						"RCODE_NAME", 
						"AA", 			
						"TC",			
						"RD",			
						"RA",			
						"Z",			
						"ANSWERS",		
						"TTLS",		
						"REJECTED"  
						)
				.filter(col("TIPO").equalTo(gc_dns));
		
		lv_dns.printSchema();
		
		System.out.println("Conexões DNS: \t"+lv_dns.count());
		
		lv_dns.show();
	}
	
	public void m_processa_http(Dataset<Row> lv_data) {

		Dataset<Row> lv_http;

		lv_http = lv_data
				  .select("TIPO",              
						"TS_CODE",  								   	
						"TS",         
						"UID",
						"ID_ORIG_H",
						"ID_ORIG_P",  
						"ID_RESP_H",  
						"ID_RESP_P",  
						"PROTO",
						"TRANS_DEPTH",  	
						"VERSION",      	
						"REQUEST_BODY_LEN",
						"RESPONSE_BODY_LEN",
						"STATUS_CODE",	
						"STATUS_MSG",		 
						"TAGS",			
						"RESP_FUIDS",		 
						"RESP_MIME_TYPES" 
						  )
				  .filter(col("TIPO").equalTo(gc_http));
		
		lv_http.printSchema();
		
		System.out.println("Conexões HTTP: \t"+lv_http.count());
		
		lv_http.show();
	}
	
}












