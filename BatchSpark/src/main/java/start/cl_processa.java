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
		
		gv_conf = new SparkConf().setMaster("local[4]").setAppName("SelectLog");
		
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
							   .load()
							   .filter(col("TS_CODE").gt(gc_stamp));
							   //.filter(col("TIPO").equalTo(gc_dns));
		
		//lv_data.createOrReplaceTempView(gv_table); //cria uma tabela temporaria, para acessar via SQL
		
		System.out.println("Conexões TOTAL: \t"+lv_data.count() + "\n\n");
		
		Dataset<Row> lv_conn;
		Dataset<Row> lv_dns;
		Dataset<Row> lv_query;
			
		lv_conn = m_processa_conn(lv_data);
		
		lv_conn.printSchema();
		
		lv_dns = m_processa_dns(lv_data);
		
		lv_query = m_dns_query(lv_dns);
		
		lv_query.printSchema();
		
		lv_query.show();
		
		m_get_conn_query(lv_conn, lv_query);
		
		//m_processa_http(lv_data);
		
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
						  "missed_bytes",	
						  "history",		
						  "orig_pkts",		
						  "orig_ip_bytes",	
						  "resp_pkts",		
						  "resp_ip_bytes",  
						  "tunnel_parents"
						  )				  
				  .filter(col("TIPO").equalTo(gc_conn));
				  
		
		System.out.println("Conexões CONN: \t"+lv_conn.count());
		
		return lv_conn;
		
		//m_conn_consumo(lv_conn);
		
		/*		String lv_sql;
		 * lv_sql = "SELECT UID, TS_CODE FROM JSON6 WHERE TIPO = 'CONN' ";
		
		System.out.println("SQL: "+lv_sql);
		
		
		lv_conn = lv_data
				.sparkSession()
				.sql(lv_sql);*/
						
		//lv_conn.printSchema();
		
		
		
		//lv_conn.show();
		
	}
	
	public void m_conn_consumo(Dataset<Row> lv_conn) {
		
		Dataset<Row> lv_res;
		
		lv_res = lv_conn.sort(col("ORIG_BYTES").desc());
		
		lv_res.show(100);
		
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
						"ANSWERS",		
						"TTLS",		
						"REJECTED"  
						)
				.filter(col("TIPO").equalTo(gc_dns));
						
		System.out.println("Conexões DNS: \t"+lv_dns.count());
		
		return lv_dns;
		
		//m_dns_query(lv_dns);	
				
		//lv_dns.printSchema();		
		
		//lv_dns.show(100);
		
	}
	
	public Dataset<Row> m_dns_query(Dataset<Row> lv_dns) {
		
		Dataset<Row> lv_res;

		lv_res = lv_dns.select(col("UID"),
							   col("ID_ORIG_H"),
							   col("ID_RESP_H"),
							   col("QUERY"))
					   .filter(col("QUERY").like("%www.%")); //equalTo("www.facebook.com"))
					   //.groupBy("QUERY")
					   //.count().sort(col("count").desc());
		
		//m_save_csv(lv_res, gc_dns);
		 
		//lv_res.show(100);
		
		return lv_res;
		
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
	
	public void m_get_conn_query(Dataset<Row> lv_conn, Dataset<Row> lv_dns) {
		
		Dataset<Row> lv_res;
		
		lv_res = lv_dns.join(lv_conn, lv_dns.col("UID").equalTo(lv_conn.col("UID")))				
				   .select(//lv_conn.col("UID"),						   
						   lv_conn.col("ID_ORIG_H"),
						   //lv_conn.col("ID_ORIG_P"),  
						   //lv_conn.col("ID_RESP_H"),  
						   lv_conn.col("ID_RESP_H"),  
						   //lv_conn.col("PROTO"),
						   //lv_conn.col("SERVICE"),	
						   //lv_conn.col("DURATION"),  
						   //lv_conn.col("ORIG_BYTES"),
						   //lv_conn.col("RESP_BYTES"),
						   lv_dns.col("QUERY")
						   )
				   .groupBy(//lv_conn.col("UID"),
						   lv_conn.col("ID_ORIG_H"),
						   //lv_conn.col("ID_RESP_H"),
						   lv_dns.col("QUERY")
						   )
				   .count()
				   .sort(col("ID_ORIG_H"),
						 col("COUNT").desc());
				 //fazer somar os bytes e gerar poor IP o total e ver o HTTP
				   /*.sort(lv_conn.col("RESP_BYTES").desc());
				   .groupBy(lv_dns.col("uid"),lv_dns.col("query"))
				   .count();*/
		
		m_save_csv(lv_res, "conn_query");
	}

	public void m_save_csv(Dataset<Row> lv_data, String lv_dir){
		
		String lv_path = gc_path_r + lv_dir;
		
		lv_data.coalesce(1) //cria apenas um arquivo
	      .write()
	      .option("header", "true")
	      .mode("overwrite") //substitui o arquivo de resultado pelo novo			
	      .csv(lv_path);
		
	}
}












