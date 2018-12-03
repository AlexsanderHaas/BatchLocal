package start;

import static org.apache.spark.sql.functions.col;

import java.util.List;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

public class cl_processa {

//---------CONSTANTES---------//
	
	final static String gc_conn_ip = "CONN_IP1";	
	
	final static String gc_conn = "CONN";
	final static String gc_dns  = "DNS";
	final static String gc_http = "HTTP";

			
//---------ATRIBUTOS---------//
	
	private cl_seleciona go_select;
	
	///-----------CASE 1 - Normal-----------////	
	
	public void m_get_all(Dataset<Row> lt_data) throws AnalysisException {
		
		Dataset<Row> lt_conn; 
		Dataset<Row> lt_dns;  
		Dataset<Row> lt_http; 
		             
		lt_conn = go_select.m_get_conn(lt_data);
		
		cl_util.m_save_csv(lt_conn, "CONN-ALL");

		cl_util.m_show_dataset(lt_conn,"CONN-ALL");
		
		lt_dns = go_select.m_get_dns(lt_data);
		
		cl_util.m_save_csv(lt_dns, "DNS-ALL");
		
		lt_http = go_select.m_get_http(lt_data);
		
		cl_util.m_save_csv(lt_http, "HTTP-ALL");
		
		m_get_www_info(lt_conn, lt_dns); //Exporta totais da conexão por filtro de WWW
		
	}
	
	public void m_conn_consumo(Dataset<Row> lv_conn) {

		Dataset<Row> lv_res;

		lv_res = lv_conn.sort(col("ORIG_BYTES").desc());

		lv_res.show(100);

	}

	public void m_get_www_info(Dataset<Row> lt_conn, Dataset<Row> lt_dns) {
		
		Dataset<Row> lt_query;
		
		lt_query = m_dns_query(lt_dns);

		m_get_conn_query(lt_conn, lt_query);

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

		//m_save_csv(lv_res, "DNS-WWW");

		cl_util.m_show_dataset(lv_res,"DNS-WWW");

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
				   						
		cl_util.m_show_dataset(lv_res,"CONN por DNS-QUERY");
				
		cl_util.m_save_csv(lv_res, "CONN-WWW");
		
		lv_res = lv_res.groupBy("QUERY")
				.sum("DURATION",
					 "ORIG_BYTES",
					 "RESP_BYTES" )
				.sort(col("sum(RESP_BYTES)").desc());
		
		
		cl_util.m_show_dataset(lv_res, "Totais de CONN por DNS-QUERY");
		
		cl_util.m_save_csv(lv_res, "CONN-WWW-SUM");	
		
	}
	
	///-----------CASE 2 - Conexões-----------////	
	
	public void m_process_orig(Dataset<Row> lt_orig, long lv_stamp) {	
		
		lt_orig = lt_orig.groupBy("ID_ORIG_H",
								  //"ID_ORIG_P",
								  "PROTO",
								  "SERVICE")
						 .sum("DURATION",
							  "ORIG_BYTES",
							  "RESP_BYTES");
		
		lt_orig = lt_orig.select(col("ID_ORIG_H"),
                                // col("ID_ORIG_P"),
                                 col("PROTO"),
                                 col("SERVICE"),
                                 col("sum(DURATION)").as("DURATION"),
				                 col("sum(ORIG_BYTES)").as("ORIG_BYTES"),
				                 col("sum(RESP_BYTES)").as("RESP_BYTES"))
		                 .withColumn("TS_CODE", functions.lit(lv_stamp));
				
		cl_util.m_save_log(lt_orig, gc_conn_ip);	
		
		cl_util.m_show_dataset(lt_orig, "Totais de CONN ORIGEM:");
		
	}
	
	public void m_process_resp(Dataset<Row> lt_resp, long lv_stamp) {
				
		lt_resp = lt_resp.groupBy("ID_RESP_H",
								  //"ID_RESP_P",
								  "PROTO",
								  "SERVICE")
						 .sum("DURATION",
							  "ORIG_BYTES",
							  "RESP_BYTES");
		
		lt_resp = lt_resp.select(col("ID_RESP_H"),
                               //col("ID_RESP_P"),
                               col("PROTO"),
                               col("SERVICE"),
                               col("sum(DURATION)").as("DURATION"),
				               col("sum(ORIG_BYTES)").as("ORIG_BYTES"),
				               col("sum(RESP_BYTES)").as("RESP_BYTES"))
		                 .withColumn("TS_CODE", functions.lit(lv_stamp));
		
		cl_util.m_save_log(lt_resp, gc_conn_ip);			
		
		cl_util.m_show_dataset(lt_resp, "Totais de CONN RESPOSTA:");
		
	}
		
	
	///-----------CASE 4 - TOTAIS-----------////

	public void m_process_totais(Dataset<Row> lt_data) {
		
		Dataset<Row> lt_total;
		
		/*lt_total = gt_data.groupBy("PROTO",
							  "ID_ORIG_H",
							  "ID_ORIG_P",
							  "ID_RESP_H",							  
						      "ID_RESP_P")					     
						 .sum("DURATION",
							  "ORIG_PKTS",
							  "ORIG_BYTES",
							  "RESP_PKTS",
							  "RESP_BYTES");*/
		
		/*gt_data.groupBy("PROTO")
			   .count().show();
		
		gt_data.groupBy("PROTO",
				        "SERVICE")
			   .count().show();*/
		
		/*lt_total = gt_data.groupBy("ID_ORIG_H")				  			     
			 .sum("DURATION",
				  "ORIG_PKTS",
				  "ORIG_BYTES",
				  "RESP_PKTS",
				  "RESP_BYTES");*/
		
		lt_total = lt_data//.filter(col("SERVICE").equalTo("http"))
						  .filter(col("ID_ORIG_H").equalTo("192.168.10.50"))
						  .groupBy("ID_ORIG_H",
				                   //"ID_ORIG_P",
				                   "ID_RESP_H",							  
			                       "ID_RESP_P")				 		  
						  .count();
				 
		
		lt_total.sort(col("COUNT").desc()).show(200);
		
		/*lt_total = gt_data.groupBy("PROTO",
				  "SERVICE")				  			     
			 .sum("DURATION",
				  "ORIG_PKTS",
				  "ORIG_BYTES",
				  "RESP_PKTS",
				  "RESP_BYTES");
		
		m_save_csv(lt_total, "CONN_TOTAIS" );*/
		
	}

	
	///-----------CASE 5 - Análises-----------////
	
	public void m_conn_proto(Dataset<Row> lt_data) {
		
		List<String> lv_fields = null;//["ID_ORIG_H];//,"ID_RESP_H","ID_RESP_P"];
		
		Column[] lv_col = new Column[(Integer) 5];
					
		lv_col[0] = new Column ("ID_ORIG_H");
		lv_col[2] = new Column ("ID_RESP_H");
		lv_col[3] = new Column ("ID_RESP_P");
		//lv_col[4] = new Column ("ID_RESP_H");
		
		lv_fields.add("ID_ORIG_H");
		//lv_fields.add(col("ID_RESP_H"));
		//lv_fields.add(col("ID_RESP_P"));
		
		Dataset<Row> lt_res;
		
		lt_data//.filter(col("SERVICE").equalTo("http"))
		  //.filter(col("ID_ORIG_H").equalTo("192.168.10.50"))
		  .groupBy(lv_col)				 		  
		  .count();
		
		
		
	}
	
}











