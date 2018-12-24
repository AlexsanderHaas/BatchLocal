package start;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

public class cl_processa {

//---------CONSTANTES---------//
	
	final String gc_totais = cl_main.gc_totais;
	
	final String gc_conn = "CONN";
	final String gc_dns  = "DNS";
	final String gc_http = "HTTP";		
		
//------------------Colunas------------------------//
	
	final String gc_proto 		 = "PROTO";
	final String gc_service		 = "SERVICE";
	final String gc_orig_h		 = "ID_ORIG_H";
	final String gc_orig_p		 = "ID_ORIG_P";
	final String gc_resp_h		 = "ID_RESP_H";
	final String gc_resp_p		 = "ID_RESP_P";
	final String gc_tipo		 = "TIPO";
	final String gc_count   	 = "COUNT";
	final String gc_ts   	 	 = "TS";
	
	
	final String gc_duration     = "DURATION";
	final String gc_o_bytes      = "ORIG_IP_BYTES";
	final String gc_r_bytes      = "RESP_IP_BYTES";
	final String gc_orig_pkts    = "ORIG_PKTS";
	final String gc_orig_bytes   = "ORIG_BYTES";
	final String gc_resp_pkts	 = "RESP_PKTS";
	final String gc_resp_bytes   = "RESP_BYTES";	
	
	final String lc_duration     = "SUM(DURATION) AS DURATION, ";
	final String lc_o_bytes      = "SUM(ORIG_IP_BYTES) AS ORIG_IP_BYTES, "; 
	final String lc_r_bytes      = "SUM(RESP_IP_BYTES) AS RESP_IP_BYTES, "; 
	final String lc_orig_pkts    = "SUM(ORIG_PKTS) AS ORIG_PKTS, ";
	final String lc_orig_bytes   = "SUM(ORIG_BYTES) AS ORIG_BYTES, ";
	final String lc_resp_pkts	 = "SUM(RESP_PKTS) AS RESP_PKTS, ";
	final String lc_resp_bytes   = "SUM(RESP_BYTES) AS RESP_BYTES ";	
	
	final String lv_sum = lc_duration   +
			              lc_o_bytes    + 
			              lc_r_bytes    +
						  lc_orig_pkts  + 
						  lc_orig_bytes +
						  lc_resp_pkts  +
						  lc_resp_bytes;	
	
	//---------TIPOS DE TOTAIS---------//
	
	final static String lc_proto 		     = "PROTO";
		
	final static String lc_proto_orig_h	 	 = "PROTO_ORIG_H";
	final static String lc_proto_orig_h_p 	 = "PROTO_ORIG_H_P";
	final static String lc_proto_resp_h	 	 = "PROTO_RESP_H";
	final static String lc_proto_resp_h_p 	 = "PROTO_RESP_H_P";
	
	final static String lc_proto_service     = "PROTO_SERVICE";
	
	final static String lc_p_s_orig_h   	 = "PROTO_SERVICE_ORIG_H";
	final static String lc_p_s_orig_h_p   	 = "PROTO_SERVICE_ORIG_H_P";
	
	final static String lc_p_s_resp_h   	 = "PROTO_SERVICE_RESP_H";
	final static String lc_p_s_resp_h_p   	 = "PROTO_SERVICE_RESP_H_P";
	
	final static String lc_service 	    	 = "SERVICE";
	
	final static String lc_orig_h	 	     = "ORIG_H";
	final static String lc_orig_p	 	     = "ORIG_P";
	final static String lc_orig_h_p	     	 = "ORIG_H_P";
	final static String lc_orig_h_p_resp_h   = "ORIG_H_P_RESP_H";
	      
	final static String lc_resp_h	 	     = "RESP_H";
	final static String lc_resp_p	 	     = "RESP_P";
	final static String lc_resp_h_p	     	 = "RESP_H_P";
	final static String lc_resp_h_p_orig_h 	 = "RESP_H_P_ORIG_H";
	      
	final static String lc_orig_h_resp_h 	 = "ORIG_H_RESP_H";
	final static String lc_orig_h_p_resp_h_p = "ORIG_H_P_RESP_H_P";
	
	//---------ATRIBUTOS---------//
	
	private cl_seleciona go_select;
	
	private long gv_stamp_filtro; //Filtro da seleção de dados
	
	private long gv_stamp; //Stamp do inicio da execução
	
	//---------METODOS---------//
		
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
		    			.filter(col("QUERY").like("%www.%"))
		    			.sort("QUERY");
		
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
			
	///-----------CASE 5 - Análises-----------////		
	
	public void m_start_analyzes(Dataset<Row> lt_data) {
		
		String lc_v = ", ";
		
		String lv_group;
				
		lt_data.createOrReplaceTempView("LOG"); //cria uma tabela temporaria, para acessar via SQL
				
//-----------Conexões por PROTOCOLO--------------------------------------//				
		
		m_group_sum(lt_data, gc_proto, lc_proto, "Conexões por Protocolo");
		
		lv_group = gc_proto + lc_v + gc_service;
		
		m_group_sum(lt_data, lv_group, lc_proto_service, "Conexões por Protocolo e Serviço");
		
		lv_group = gc_proto + lc_v + gc_orig_h;
		
		m_group_sum(lt_data, lv_group, lc_proto_orig_h, "Conexões por Protocolo e IP Origem");	
		
		lv_group = gc_proto + lc_v + gc_orig_h + lc_v + gc_orig_p;
		
		m_group_sum(lt_data, lv_group, lc_proto_orig_h_p, "Conexões por Protocolo, IP e Porta Origem");	
		
		lv_group = gc_proto + lc_v + gc_resp_h;
		
		m_group_sum(lt_data, lv_group, lc_proto_resp_h, "Conexões por Protocolo e IP Resposta");	
		
		lv_group = gc_proto + lc_v + gc_resp_h + lc_v + gc_resp_p;
		
		m_group_sum(lt_data, lv_group, lc_proto_resp_h_p, "Conexões por Protocolo, IP e Porta Resposta");
		
		
//-----------Conexões por SERVIÇO--------------------------------------//
		
		m_group_sum(lt_data, gc_service, lc_service, "Conexões por Serviço");	
		
		lv_group =  gc_proto + lc_v + gc_service + lc_v + gc_orig_h;
		
		m_group_sum(lt_data, lv_group, lc_p_s_orig_h, "Conexões por Protocolo, Serviço e IP Origem");
		
		lv_group =  gc_proto + lc_v + gc_service + lc_v + gc_orig_h + lc_v + gc_orig_p;
		
		m_group_sum(lt_data, lv_group, lc_p_s_orig_h_p, "Conexões por Protocolo, Serviço, IP e Porta Origem\"");
		
		lv_group =  gc_proto + lc_v + gc_service + lc_v + gc_resp_h;
		
		m_group_sum(lt_data, lv_group, lc_p_s_resp_h, "Conexões por Protocolo, Serviço e IP Resposta");
		
		lv_group =  gc_proto + lc_v + gc_service + lc_v + gc_resp_h + lc_v + gc_resp_p;
		
		m_group_sum(lt_data, lv_group, lc_p_s_resp_h_p, "Conexões por Protocolo, Serviço, IP e Porta Resposta");
		
//-----------Conexões IP Origem--------------------------------------//
		
		m_group_sum(lt_data, gc_orig_h, lc_orig_h, "Conexões por IP Origem");
						
		m_group_sum(lt_data, gc_orig_p, lc_orig_p,"Conexões por Portas Origem");	
				
		lv_group = gc_orig_h + lc_v + gc_orig_p; 
				
		m_group_sum(lt_data, lv_group, lc_orig_h_p, "Conexões por IP e Porta Origem");			
								
		lv_group = gc_orig_h + lc_v + gc_orig_p + lc_v + gc_resp_h;
		
		m_group_sum(lt_data, lv_group, lc_orig_h_p_resp_h,"Conexões por IP Origem e Porta com IP Resposta");
		
		
//-----------Conexões IP Resposta--------------------------------------//
		
		m_group_sum(lt_data, gc_resp_h, lc_resp_h, "Conexões por IP Resposta");
		
		m_group_sum(lt_data, gc_resp_p, lc_resp_p, "Conexões por Portas Resposta");
		
		lv_group = gc_resp_h + lc_v + gc_resp_p;
		
		m_group_sum(lt_data, lv_group, lc_resp_h_p, "Conexões por IP e Porta Resposta");		
				
		lv_group = gc_resp_h + lc_v + gc_resp_p + lc_v + gc_orig_h;
		
		m_group_sum(lt_data, lv_group, lc_resp_h_p_orig_h, "Conexões por IP Resposta e Porta com IP Origem");
		
//-----------Conexões IP Origem com IP Resposta--------------------------------------//
		
		lv_group = gc_orig_h + lc_v + gc_resp_h;
		
		m_group_sum(lt_data, lv_group, lc_orig_h_resp_h, "Conexões por IP Origem e IP Resposta");		
		
		lv_group = gc_orig_h + lc_v + gc_orig_p + lc_v + gc_resp_h + lc_v + gc_resp_p;
		
		m_group_sum(lt_data, lv_group, lc_orig_h_p_resp_h_p, "Conexões por IP Origem e Porta com IP Resposta e Porta");
		
	}
		
	public void m_group_sum(Dataset<Row> lt_data, 									
						    String   lv_group, 
						    String   lv_tipo,
						    String   lv_desc) {
		
		final String lc_table = "LOG"; 
				
		String ts_h = "DATE_FORMAT(TS,'dd.MM.yyyy HH') AS TS";
		
		String lv_grp = ts_h + ", " + lv_group 
			            + ", COUNT(*) AS COUNT, ";
		
		lv_group = lv_group + ", DATE_FORMAT(TS,'dd.MM.yyyy HH')"; 
		
		String lv_sql = "SELECT " +
						lv_grp    +
						lv_sum    +
						"FROM "   + lc_table +
						" GROUP BY " + lv_group;
		
		Dataset<Row> lt_res;
		
		cl_util.m_time_start();							
			
		//System.out.println("SQL: "+lv_sql);
				
		lt_res = lt_data.sparkSession()
						  .sql(lv_sql);	
		
		lt_res = lt_res.withColumn("TIPO", functions.lit(lv_tipo))
				       .withColumn("TS_FILTRO", functions.lit(gv_stamp_filtro))						   
				       .withColumn("TS_CODE", functions.lit(gv_stamp))
				       .withColumn("ROW_ID", functions.monotonically_increasing_id())
				       .withColumn(gc_ts, to_timestamp(col(gc_ts), "dd.MM.yyyy HH")); //para salvar no banco coloca em timestamp novamente;
		
		cl_util.m_show_dataset(lt_res, lv_desc + "-RES:");
		
		cl_util.m_save_log(lt_res, gc_totais);
		
		cl_util.m_time_end();	
			
	}
	
	///-----------CASE 8 - Export Results-----------////
	
	public void m_export_totais(Dataset<Row> lt_data) {
		
		cl_util.m_save_csv(lt_data.select(gc_proto, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_proto)), lc_proto);
		
		cl_util.m_save_csv(lt_data.select(gc_proto, gc_orig_h, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_proto_orig_h))
								  .sort(gc_proto, gc_orig_h), lc_proto_orig_h);
		
		cl_util.m_save_csv(lt_data.select(gc_proto, gc_orig_h, gc_orig_p, gc_count)
				  				  .filter(col(gc_tipo).equalTo(lc_proto_orig_h_p))
				  				  .sort(gc_proto, gc_orig_h, gc_orig_p ), lc_proto_orig_h_p);
		
		cl_util.m_save_csv(lt_data.select(gc_proto, gc_resp_h, gc_count)
			      				  .filter(col(gc_tipo).equalTo(lc_proto_resp_h))
   		      					  .sort(gc_proto, gc_resp_h), lc_proto_resp_h);
		
		cl_util.m_save_csv(lt_data.select(gc_proto, gc_resp_h, gc_resp_p, gc_count)
				  				  .filter(col(gc_tipo).equalTo(lc_proto_resp_h_p))
				  				  .sort(gc_proto, gc_resp_h, gc_resp_p), lc_proto_resp_h_p);
		
		cl_util.m_save_csv(lt_data.select(gc_service, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_service)), lc_service);
		
		cl_util.m_save_csv(lt_data.select(gc_proto, gc_service, gc_count)
				  				  .filter(col(gc_tipo).equalTo(lc_proto_service))
								  .sort(gc_proto, gc_service), lc_proto_service);
		
		cl_util.m_save_csv(lt_data.select(gc_proto, gc_service, gc_orig_h, gc_count)
				  				  .filter(col(gc_tipo).equalTo(lc_p_s_orig_h))
				  				  .sort(gc_proto, gc_service, gc_orig_h), lc_p_s_orig_h);
		
		cl_util.m_save_csv(lt_data.select(gc_proto, gc_service, gc_orig_h, gc_orig_p, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_p_s_orig_h_p))
								  .sort(gc_proto, gc_service, gc_orig_h, gc_orig_p), lc_p_s_orig_h_p);
		
		cl_util.m_save_csv(lt_data.select(gc_proto, gc_service, gc_resp_h, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_p_s_resp_h))
								  .sort(gc_proto, gc_service, gc_resp_h), lc_p_s_resp_h);
		
		cl_util.m_save_csv(lt_data.select(gc_proto, gc_service, gc_resp_h, gc_resp_p, gc_count)
				  				  .filter(col(gc_tipo).equalTo(lc_p_s_resp_h_p))
				  				  .sort(gc_proto, gc_service, gc_resp_h, gc_resp_p), lc_p_s_resp_h_p);
		
		//-----------Conexões IP Origem--------------------------------------//
		
		cl_util.m_save_csv(lt_data.select(gc_orig_h, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_orig_h))
								  .sort(gc_orig_h), lc_orig_h);
		
		cl_util.m_save_csv(lt_data.select(gc_orig_p, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_orig_p))
								  .sort(gc_orig_p), lc_orig_p);
		
		cl_util.m_save_csv(lt_data.select(gc_orig_h, gc_orig_p, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_orig_h_p))
								  .sort(gc_orig_h, gc_orig_p), lc_orig_h_p);		
		
		cl_util.m_save_csv(lt_data.select(gc_orig_h, gc_orig_p, gc_resp_h, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_orig_h_p_resp_h ))
								  .sort(gc_orig_h, gc_orig_p), lc_orig_h_p_resp_h );
		
		//-----------Conexões IP Resposta--------------------------------------//
		
		cl_util.m_save_csv(lt_data.select(gc_resp_h, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_resp_h))
								  .sort(gc_resp_h), lc_resp_h);
		
		cl_util.m_save_csv(lt_data.select(gc_resp_p, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_resp_p))
								  .sort(gc_resp_p), lc_resp_p);
		
		cl_util.m_save_csv(lt_data.select(gc_resp_h, gc_resp_p, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_resp_h_p))
								  .sort(gc_resp_h, gc_resp_p), lc_resp_h_p);

		cl_util.m_save_csv(lt_data.select(gc_resp_h, gc_resp_p, gc_orig_h, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_resp_h_p_orig_h ))
								  .sort(gc_resp_h, gc_resp_p), lc_resp_h_p_orig_h );
				
		
		//-----------Conexões IP Origem com IP Resposta------------------------//
		
		cl_util.m_save_csv(lt_data.select(gc_orig_h, gc_resp_h, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_orig_h_resp_h))
								  .sort(gc_orig_h, gc_resp_h), lc_orig_h_resp_h);
		
		cl_util.m_save_csv(lt_data.select(gc_orig_h, gc_orig_p, gc_resp_h, gc_resp_p, gc_count)
								  .filter(col(gc_tipo).equalTo(lc_orig_h_p_resp_h_p))
								  .sort(gc_orig_h, gc_orig_p), lc_orig_h_p_resp_h_p);
		
	}	
	
}


