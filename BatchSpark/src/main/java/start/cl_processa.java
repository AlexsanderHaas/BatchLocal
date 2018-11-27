package start;

import static org.apache.spark.sql.functions.col;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

public class cl_processa {

//---------CONSTANTES---------//
	final static String gv_table = "JSON9";
	final static String gv_zkurl = "localhost:2181";
	
	final static String gc_conn = "CONN";
	final static String gc_dns  = "DNS";
	final static String gc_http = "HTTP";
	
	final static String gc_stamp = "2018-11-25 15:53:00.000";
	
	final static String gc_path_r = "/home/user/Documentos/batch_spark/";
	
//---------ATRIBUTOS---------//
	private static cl_processa gv_processa;

	private static Map<String, String> gv_phoenix;
	
	private static SparkConf gv_conf;
    
	private static SparkContext gv_context;
	                        
	private static SparkSession gv_session;
	
	private Dataset<Row> gt_data;	
	private Dataset<Row> gt_conn;
	private Dataset<Row> gt_dns;
	private Dataset<Row> gt_http;
	
	public static void main(String[] args) throws AnalysisException {
	
		gv_processa = new cl_processa();
		
		gv_processa.m_start();
	}
	
	public void m_start() throws AnalysisException {
						
		m_conecta_phoenix();
		
		m_seleciona();
		
		if(gt_data.count() > 0) {
		
			m_processa();

		}
	}
	
	public static void m_conecta_phoenix(){
		
		gv_phoenix = new HashMap<String, String>();
		
		gv_phoenix.put("zkUrl", gv_zkurl);
		gv_phoenix.put("hbase.zookeeper.quorum", "master");
		gv_phoenix.put("table", gv_table);
		
		gv_conf = new SparkConf().setMaster("local[4]").setAppName("SelectLog");
		
		gv_context = new SparkContext(gv_conf);
		
		gv_session = new SparkSession(gv_context);		
		
		Logger.getRootLogger().setLevel(Level.ERROR);
	}
	
	public void m_seleciona() throws AnalysisException {
		
		gt_data = gv_session
			      .sqlContext()
			      .read()
			      .format("org.apache.phoenix.spark")
			      .options(gv_phoenix)							   
			      .load().sort("UID");
			      //.filter("TIPO = 'CONN' OR TIPO = 'DNS'");//filter("TS_CODE = TO_TIMESTAMP ('"+gc_stamp+"')"); //" AND ( TIPO = 'CONN' OR TIPO = 'DNS' )");
			      /*.filter(col("TS_CODE").gt(gc_stamp))
			      .filter(col("TIPO").equalTo(gc_conn));*/
							   
		
		//lv_data.createOrReplaceTempView(gv_table); //cria uma tabela temporaria, para acessar via SQL
		
		System.out.println("Conexões TOTAL: \t"+gt_data.count() + "\n\n");
				
	}
	
	public void m_processa() throws AnalysisException {
				
		gt_conn = m_processa_conn(gt_data);
		
		m_save_csv(gt_conn, "CONN-ALL");

		//m_show_dataset(gt_conn,"CONN-ALL");
		
		gt_dns = m_processa_dns(gt_data);
		m_save_csv(gt_dns, "DNS-ALL");
		
		gt_http = m_processa_http(gt_data);
		m_save_csv(gt_http, "HTTP-ALL");
		
		//m_get_www_info(); //Exporta totais da conexão por filtro de WWW
		

	}
	public Dataset<Row> m_processa_conn(Dataset<Row> lv_data) throws AnalysisException {
		
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
						  "MISSED_BYTES",	
						  "HISTORY",		
						  "ORIG_PKTS",		
						  "ORIG_IP_BYTES",	
						  "RESP_PKTS",		
						  "RESP_IP_BYTES"  
						 // "TUNNEL_PARENTS"
						  )				  
				  .filter(col("TIPO").equalTo(gc_conn)).limit(10000);
				  
		
		System.out.println("Conexões CONN: \t"+lv_conn.count());
		
		return lv_conn;
		
		//m_conn_consumo(lv_conn);
		
		/*		String lv_sql;
		 * lv_sql = "SELECT UID, TS_CODE FROM JSON6 WHERE TIPO = 'CONN' ";
		
		System.out.println("SQL: "+lv_sql);
		
		
		lv_conn = lv_data
				.sparkSession()
				.sql(lv_sql);*/
						
		//m_show_dataset(lv_conn);
		
	}
	
	public Dataset<Row> m_processa_dns(Dataset<Row> lv_data) {

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
						//"ANSWERS",		
						//"TTLS",		
						"REJECTED"  
						)
				.filter(col("TIPO").equalTo(gc_dns)).limit(10000);
						
		System.out.println("Conexões DNS: \t"+lv_dns.count());
		
		return lv_dns;
		
		//m_show_dataset(lv_dns);
		
	}
			
	public Dataset<Row> m_processa_http(Dataset<Row> lv_data) {

		Dataset<Row> lt_http;

		lt_http = lv_data
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
						"STATUS_MSG"		 
						//"TAGS",			
						//"RESP_FUIDS",		 
						//"RESP_MIME_TYPES" 
						  )
				  .filter(col("TIPO").equalTo(gc_http)).limit(10000);			
		
		System.out.println("Conexões HTTP: \t"+lt_http.count());
		
		return lt_http;
		
	}
	
	public void m_conn_consumo(Dataset<Row> lv_conn) {

		Dataset<Row> lv_res;

		lv_res = lv_conn.sort(col("ORIG_BYTES").desc());

		lv_res.show(100);

	}

	public void m_get_www_info() {
		
		Dataset<Row> lt_query;
		
		lt_query = m_dns_query(gt_dns);

		m_get_conn_query(gt_conn, lt_query);

	}
	
	public Dataset<Row> m_dns_query(Dataset<Row> lv_dns) {

		Dataset<Row> lv_res;

		lv_res = lv_dns.select("UID",
							   "ID_ORIG_H",
							   "ID_ORIG_P",
							   "ID_RESP_H",
							   "ID_RESP_P",
							   "QUERY")
				// col("ANSWERS"))
				.filter(col("QUERY").like("%www.%"))//.limit(1000); // equalTo("www.facebook.com"))
				.sort("QUERY");
		
		// .groupBy("QUERY")
		// .count().sort(col("count").desc());

		m_save_csv(lv_res, "DNS-WWW");

		m_show_dataset(lv_res,"DNS-WWW");

		return lv_res;

	}
	
	public void m_get_conn_query(Dataset<Row> lv_conn, Dataset<Row> lv_dns) {
			
		Dataset<Row> lv_res;
		
		lv_res = lv_dns.as("dns")				 
				 .join(lv_conn.as("conn"), "UID")							  
				 .select("UID",
						 "conn.ID_ORIG_H",
						 "conn.ID_ORIG_P",  
						 "conn.ID_RESP_H", 
						 "conn.ID_RESP_P",  
						 "PROTO",
						 "SERVICE",	
						 "DURATION",  
						 "ORIG_BYTES",
						 "RESP_BYTES",
						 "QUERY"						   						  
						 )
				   .distinct()				   
				   .sort(col("ORIG_BYTES").desc());
				   						
		m_show_dataset(lv_res,"CONN por DNS-QUERY");
				
		m_save_csv(lv_res, "CONN-WWW");
		
		lv_res = lv_res.groupBy("QUERY")
				.sum("DURATION",
					 "ORIG_BYTES",
					 "RESP_BYTES" )
				.sort(col("sum(RESP_BYTES)").desc());
		
		
		m_show_dataset(lv_res, "Totais de CONN por DNS-QUERY");
		
		m_save_csv(lv_res, "CONN-WWW-SUM");	
		
	}
	
	public void m_show_dataset(Dataset<Row> lv_data, String lv_desc) {
		
		System.out.println("\nConexões - " + lv_desc + "\t" + lv_data.count());
		
		//lv_data.printSchema();
		
		//lv_data.show();
						
	}
	
	public void m_save_csv(Dataset<Row> lv_data, String lv_dir){
		
		String lv_path = gc_path_r + lv_dir;
		
		lv_data.coalesce(1) //cria apenas um arquivo
	      .write()
	      .option("header", "true")
	      .mode("overwrite") //substitui o arquivo de resultado pelo novo			
	      //.json(lv_path);
	      .csv(lv_path);
		
	}
}












