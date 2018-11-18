package start;

import static org.apache.spark.sql.functions.col;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class cl_processa {
	
	final static String gv_table = "JSON6";
	final static String gv_zkurl = "localhost:2181";
	
	final static String gc_conn = "CONN";
	final static String gc_dns  = "DNS";
	final static String gc_http = "HTTP";
	
	private static cl_processa gv_processa;

	private static Map<String, String> gv_phoenix;
	
	private static SparkConf gv_conf;
    
	private static SparkContext gv_context;
	                        
	private static SparkSession gv_session;
	
	public static void main(String[] args) {
	
		gv_processa = new cl_processa();
		
		gv_processa.m_start();
	}
	
	public void m_start() {
						
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
	
	public void m_seleciona() {
							
		Dataset<Row> lv_data = gv_session
							   .sqlContext()
							   .read()
							   .format("org.apache.phoenix.spark")
							   .options(gv_phoenix)
							   .load();
		
		System.out.println("Conexões TOTAL: \t"+lv_data.count() + "\n\n");
		
		m_processa_conn(lv_data);
		m_processa_dns(lv_data);
		m_processa_http(lv_data);
		
	}
	
	public void m_processa_conn(Dataset<Row> lv_data) {
		
		Dataset<Row> lv_conn;
		
		lv_conn = lv_data
				  .filter(col("TIPO")
				  .equalTo(gc_conn));
		
		System.out.println("Conexões CONN: \t"+lv_conn.count());
		
	}
	
	public void m_processa_dns(Dataset<Row> lv_data) {

		Dataset<Row> lv_dns;

		lv_dns = lv_data
				.filter(col("TIPO")
				.equalTo(gc_dns));
		
		System.out.println("Conexões DNS: \t"+lv_dns.count());
		
	}
	
	public void m_processa_http(Dataset<Row> lv_data) {

		Dataset<Row> lv_http;

		lv_http = lv_data
				  .filter(col("TIPO")
				  .equalTo(gc_http));
		
		System.out.println("Conexões HTTP: \t"+lv_http.count());
		
	}
	
}












