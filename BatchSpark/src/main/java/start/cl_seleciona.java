package start;

import static org.apache.spark.sql.functions.col;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class cl_seleciona {

	//---------CONSTANTES---------//
	
	final public static String gc_zkurl = "localhost:2181";
	
	final public static String gc_phoenix = "org.apache.phoenix.spark";
	
	final static String gc_conn = "CONN";
	final static String gc_dns  = "DNS";
	final static String gc_http = "HTTP";
	
	//---------ATRIBUTOS---------//
	
	private Map<String, String> gv_phoenix;
		                        
	private SparkSession gv_session;
				
	public void m_conf_phoenix(String lv_table, SparkSession lv_session){
		
		gv_session = lv_session;
		
		gv_phoenix = new HashMap<String, String>();
		
		gv_phoenix.put("zkUrl", gc_zkurl);
		gv_phoenix.put("hbase.zookeeper.quorum", "master");
		gv_phoenix.put("table", lv_table);
		
	}
		
	public Dataset<Row> m_seleciona(String lv_stamp) throws AnalysisException {
		
		Dataset<Row> lt_data;	
		
		lt_data = gv_session
			      .sqlContext()
			      .read()
			      .format(gc_phoenix)
			      .options(gv_phoenix)							   
			      .load()			      
			      //.filter("TIPO = 'CONN' OR TIPO = 'DNS'");//filter("TS_CODE = TO_TIMESTAMP ('"+gc_stamp+"')"); //" AND ( TIPO = 'CONN' OR TIPO = 'DNS' )");
			      .filter(col("TS_CODE").gt(lv_stamp)); //.filter(col(cl_processa.gc_service).equalTo("http"));
			      //.filter(col("TIPO").equalTo(gc_conn));*/
							   		
		//lv_data.createOrReplaceTempView(gv_table); //cria uma tabela temporaria, para acessar via SQL
		
		System.out.println("Conexões TOTAL: \t"+lt_data.count() + "\n\n");
		
		return lt_data;
				
	}

	public Dataset<Row> m_get_conn(Dataset<Row> lv_data) throws AnalysisException {
		
		Dataset<Row> lt_conn;	
		
		lt_conn = lv_data
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
				  		
		cl_util.m_show_dataset(lt_conn,"Conexões CONN:");
		
		return lt_conn;
		
		//m_conn_consumo(lv_conn);
		
		/*		String lv_sql;
		 * lv_sql = "SELECT UID, TS_CODE FROM JSON6 WHERE TIPO = 'CONN' ";
		
		System.out.println("SQL: "+lv_sql);
		
		
		lv_conn = lv_data
				.sparkSession()
				.sql(lv_sql);*/								
		
	}
	
	public Dataset<Row> m_get_dns(Dataset<Row> lv_data) {

		Dataset<Row> lt_dns;
		
		lt_dns = lv_data
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
								
		cl_util.m_show_dataset(lt_dns,"Conexões DNS:");
		
		return lt_dns;		
		
	}
			
	public Dataset<Row> m_get_http(Dataset<Row> lv_data) {

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
		
		//System.out.println("Conexões HTTP: \t"+lt_http.count());
		
		cl_util.m_show_dataset(lt_http,"Conexões HTTP:");
		
		return lt_http;
		
	}	

	public Dataset<Row> m_seleciona_conn(String lv_stamp){
		
		Dataset<Row> lt_data;
		
		lt_data = gv_session
			      .sqlContext()
			      .read()
			      .format(gc_phoenix)
			      .options(gv_phoenix)							   
			      .load()			      
			      .filter(col("TIPO").equalTo(gc_conn))
			      .filter(col("TS_CODE").gt(lv_stamp))			      
				  .sort(col("TS_CODE").desc());					   
	
		/*Dataset<Row> lt_orig;	
		
		lt_orig = gt_data.groupBy("ID_ORIG_H",
								  "ID_ORIG_P",
								  "PROTO",
								  "SERVICE").count();
		
		System.out.println("Conexões TOTAL: \t"+ lt_orig.count() + "\n\n");
		
		lt_orig.show();*/
		
		return lt_data;
				
	}

	public Dataset<Row> m_seleciona_results(String lv_stamp, String lv_tipo){
		
		Dataset<Row> lt_data;	
		
		lt_data = gv_session
			      .sqlContext()
			      .read()
			      .format(gc_phoenix)
			      .options(gv_phoenix)							   
			      .load()			      			      
			      .filter(col("TIPO").equalTo(lv_tipo))
				  .filter(col("TS_CODE").gt(lv_stamp))
				  .filter(col("COUNT").isNull());
			     		
		//System.out.println("Conexões Resultado: \t"+lt_data.count() + "\n\n");
		
		return lt_data;
		
	}
	
	public Dataset<Row> m_select_LogTotais(String lv_stamp){
		
		Dataset<Row> lt_data;	
		
		lt_data = gv_session
			      .sqlContext()
			      .read()
			      .format(gc_phoenix)
			      .options(gv_phoenix)							   
			      .load()			      			      
				  .filter(col("TS_CODE").gt(lv_stamp));
			     		
		//cl_util.m_show_dataset(lt_data, ":LOG totais");
		
		return lt_data;
		
	}
	
	public Dataset<Row> m_select_LogKmeans(String lv_stamp){
		
		Dataset<Row> lt_data;	
		
		lt_data = gv_session
			      .sqlContext()
			      .read()
			      .format(gc_phoenix)
			      .options(gv_phoenix)							   
			      .load()			      			      
				  .filter(col(cl_kmeans.gc_ts_code).gt(lv_stamp))
				  .filter(col("ID_RESP_H").equalTo("23.111.11.211")).limit(2);
			     		
		//cl_util.m_show_dataset(lt_data, ":LOG totais");
		
		return lt_data;
		
	}
	
	public Dataset<Row> m_select_IpInfo(Dataset<Row> lt_data, String lv_field) {
		
		Dataset<Row> lt_res = gv_session
			                  .sqlContext()
			                  .read()
			                  .format(gc_phoenix)
			                  .options(gv_phoenix)					   
			                  .load()
			                  .join(lt_data,col(lv_field).equalTo(col("IP")),"inner");
			                  //.filter(col("IP").equalTo(lt_data.col(lv_field)));
				
		return lt_res;
	}
	
}
