package start;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import scala.collection.Seq;
import scala.collection.JavaConverters;

//import org.apache.spark.ml.evaluation.ClusteringEvaluator;

import static org.apache.spark.sql.functions.col;

public class cl_processa {

//---------CONSTANTES---------//
		
	final static String gc_conn_ip = "CONN_IP1";	
	
	final static String gc_analyzes = "LOG_ANALYZES2";
	
	final static String gc_conn = "CONN";
	final static String gc_dns  = "DNS";
	final static String gc_http = "HTTP";
	
//------------------Colunas------------------------//
	
	final static String gc_proto 		= "PROTO";
	final static String gc_service		= "SERVICE";
	final static String gc_orig_h		= "ID_ORIG_H";
	final static String gc_orig_p		= "ID_ORIG_P";
	final static String gc_resp_h		= "ID_RESP_H";
	final static String gc_resp_p		= "ID_RESP_P";
	
	final static String gc_duration     = "DURATION";
	final static String gc_orig_pkts    = "ORIG_PKTS";
	final static String gc_orig_bytes   = "ORIG_BYTES";
	final static String gc_resp_pkts	= "RESP_PKTS";
	final static String gc_resp_bytes   = "RESP_BYTES";						
	
//---------ATRIBUTOS---------//
	
	private cl_seleciona go_select;
	
	private long gv_stamp_filtro; //Filtro da seleção de dados
	
	private long gv_stamp; //Stamp do inicio da execução
	
	public cl_processa(String lv_filtro, long lv_stamp){
		
		java.sql.Timestamp lv_ts = java.sql.Timestamp.valueOf( lv_filtro ) ;
		
		gv_stamp_filtro = lv_ts.getTime(); 
		
		gv_stamp = lv_stamp;
		
	}
	
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
	
	public void m_start_analyzes(Dataset<Row> lt_data) {
		
		final String lc_proto 		     = "PROTO";
		final String lc_service 	     = "SERVICE";
		final String lc_orig_h	 	     = "ORIG_H";
		final String lc_orig_p	 	     = "ORIG_P";
		final String lc_orig_h_p	     = "ORIG_H_P";
		final String lc_orig_h_proto	 = "ORIG_H_PROTO";
		final String lc_orig_h_service	 = "ORIG_H_SERVICE";
		final String lc_orig_h_p_resp_h  = "ORIG_H_P_RESP_H";
		
		final String lc_resp_h	 	     = "RESP_H";
		final String lc_resp_p	 	     = "RESP_P";
		final String lc_resp_h_p	     = "RESP_H_P";
		final String lc_resp_h_proto	 = "RESP_H_PROTO";
		final String lc_resp_h_service	 = "RESP_H_SERVICE";
		final String lc_resp_h_p_orig_h  = "RESP_H_P_ORIG_H";
		
		final String lc_orig_h_resp_h 	  = "ORIG_H_RESP_H";
		final String lc_orig_h_p_resp_h_p = "ORIG_H_P_RESP_H_P";
										
		//Colunas a somar
		
		String[] lv_sum = new String[5]; //Colunas a somar
		            
		lv_sum[0] = gc_duration;
		lv_sum[1] = gc_orig_pkts;
		lv_sum[2] = gc_orig_bytes;
		lv_sum[3] = gc_resp_pkts;
	    lv_sum[4] = gc_resp_bytes;
		
		Column[] lv_group = new Column[(Integer) 1]; //Colunas a agrupar (Não pode deixar posições vazias)
		
//-----------Conexões por PROTOCOLO--------------------------------------//
		
		lv_group[0] = new Column(gc_proto);
		
		m_group_sum(lt_data, gc_proto, lc_proto, "Conexões por Protocolo");
		
/*//-----------Conexões por SERVIÇO--------------------------------------//
		
		lv_group[0] = new Column(gc_service);
				
		m_group_sum(lt_data, lv_group, lv_sum, lc_service, "Conexões por Serviço");	
		
//-----------Conexões IP Origem--------------------------------------//
		
		lv_group[0] = new Column(gc_orig_h);
						
		m_group_sum(lt_data, lv_group, lv_sum, lc_orig_h, "Conexões por IP Origem");
		
		lv_group[0] = new Column(gc_orig_p);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_orig_p,"Conexões por Portas Origem");	
		
		lv_group = new Column[(Integer) 2];
		
		lv_group[0] = new Column(gc_orig_h);
		
		lv_group[1] = new Column(gc_orig_p);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_orig_h_p, "Conexões por IP e Porta Origem");
		
		lv_group[0] = new Column(gc_orig_h);
		
		lv_group[1] = new Column(gc_proto);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_orig_h_proto, "Conexões por IP Origem e Protocolo");
				
		lv_group[0] = new Column(gc_orig_h);
		
		lv_group[1] = new Column(gc_service);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_orig_h_service, "Conexões por IP Origem e Serviço");
						
		lv_group = new Column[(Integer) 3];
		
		lv_group[0] = new Column(gc_orig_h);
		
		lv_group[1] = new Column(gc_orig_p);
		
		lv_group[2] = new Column(gc_resp_h);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_orig_h_p_resp_h,"Conexões por IP Origem e Porta com IP Resposta");
		
		
//-----------Conexões IP Resposta--------------------------------------//
		
		lv_group = new Column[(Integer) 1];
		
		lv_group[0] = new Column(gc_resp_h);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_resp_h, "Conexões por IP Resposta");
		
		lv_group[0] = new Column(gc_resp_p);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_resp_p, "Conexões por Portas Resposta");
		
		lv_group = new Column[(Integer) 2];
		
		lv_group[0] = new Column(gc_resp_h);
		
		lv_group[1] = new Column(gc_resp_p);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_resp_h_p, "Conexões por IP e Porta Resposta");
		
		lv_group[0] = new Column(gc_resp_h);
		
		lv_group[1] = new Column(gc_proto);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_resp_h_proto, "Conexões por IP Resposta e Protocolo");
		
		lv_group[0] = new Column(gc_resp_h);
		
		lv_group[1] = new Column(gc_service);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_resp_h_service, "Conexões por IP Resposta e Serviço");
		
		lv_group = new Column[(Integer) 3];
		
		lv_group[0] = new Column(gc_resp_h);
		
		lv_group[1] = new Column(gc_resp_p);
		
		lv_group[2] = new Column(gc_orig_h);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_resp_h_p_orig_h, "Conexões por IP Resposta e Porta com IP Origem");
		
//-----------Conexões IP Origem com IP Resposta--------------------------------------//
		
		lv_group = new Column[(Integer) 2];
		
		lv_group[0] = new Column(gc_orig_h);
		
		lv_group[1] = new Column(gc_resp_h);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_orig_h_resp_h, "Conexões por IP Origem e IP Resposta");		
		
		lv_group = new Column[(Integer) 4];
		
		lv_group[0] = new Column(gc_orig_h);
		
		lv_group[1] = new Column(gc_orig_p);
		
		lv_group[2] = new Column(gc_resp_h);
		
		lv_group[3] = new Column(gc_resp_p);
		
		m_group_sum(lt_data, lv_group, lv_sum, lc_orig_h_p_resp_h_p, "Conexões por IP Origem e Porta com IP Resposta e Porta");*/
		
	}
		
	public void m_group_sum(Dataset<Row> lt_data, 									
						    String   lv_group, 
						    String   lv_tipo,
						    String   lv_desc) {
		
		final String lc_table = "LOG"; 
		
		final String lc_duration     = "SUM(DURATION) AS DURATION, ";
		final String lc_orig_pkts    = "SUM(ORIG_PKTS) AS ORIG_PKTS, ";
		final String lc_orig_bytes   = "SUM(ORIG_BYTES) AS ORIG_BYTES, ";
		final String lc_resp_pkts	 = "SUM(RESP_PKTS) AS RESP_PKTS, ";
		final String lc_resp_bytes   = "SUM(RESP_BYTES) AS RESP_BYTES ";	
		
		final String lv_sum = lc_duration   + 
							  lc_orig_pkts  + 
							  lc_orig_bytes +
							  lc_resp_pkts  +
							  lc_resp_bytes;
		
		String lv_grp = lv_group + ", COUNT(*) AS COUNT, ";
		
		String lv_sql = "SELECT " +
						lv_grp    +
						lv_sum    +
						"FROM "   + lc_table +
						" GROUP BY "+ lv_group;
		
		Dataset<Row> lt_res;
		
		long lv_i = System.currentTimeMillis();  
				
		lv_i = System.currentTimeMillis();  			
		
		lt_data.createOrReplaceTempView("LOG"); //cria uma tabela temporaria, para acessar via SQL				
			
		System.out.println("SQL: "+lv_sql);
				
		lt_res = lt_data.sparkSession()
						  .sql(lv_sql);	
		
		lt_res = lt_res.withColumn("TIPO", functions.lit(lv_tipo))
				       .withColumn("TS_FILTRO", functions.lit(gv_stamp_filtro))						   
				       .withColumn("TS_CODE", functions.lit(gv_stamp))
				       .withColumn("ROW_ID", functions.monotonically_increasing_id());
		
		cl_util.m_show_dataset(lt_res, lv_desc + "-JOIN:");
		
		long lv_f = ( System.currentTimeMillis() - lv_i ) / 1000;
		
		System.out.println(lv_tipo+") A função foi executada em:\t" + lv_f +" Segundos");
			
	}
	
}











