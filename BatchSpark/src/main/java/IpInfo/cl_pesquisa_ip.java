package IpInfo;

//import org.apache.spark.api.java.function.MapFunction;
//import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

import io.ipinfo.api.IPInfo;
import io.ipinfo.api.model.IPResponse;
import start.cl_seleciona;
import start.cl_util;

/*import io.ipinfo.api.IPInfo;
import io.ipinfo.api.errors.RateLimitedException;
import io.ipinfo.api.model.IPResponse;*/

public class cl_pesquisa_ip {
	
	//---------CONSTANTES---------//
		
	final String gc_table = "IP_INFO";
	
	final String gc_ts	  = "TS_CODE";
	
	final String gc_ip	  = "IP";
	
	//---------ATRIBUTOS---------//
	
	private SparkSession gv_session;
	
	private long gv_stamp;
	
	private cl_seleciona go_select;
	
	private String gv_field;
	
	//---------METODOS---------//
	
	public cl_pesquisa_ip(SparkSession lv_session, long lv_stamp) {
		
		gv_session = lv_session;
		
		gv_stamp = lv_stamp;
		
	}
	
	public Dataset<Row> m_processa_ip(Dataset<Row> lt_data, String lv_field) {
		
		Dataset<Row> lt_ips;
		
		Dataset<Row> lt_ips_hb;
		
		Dataset<Row> lt_ips_nf;
		
		Dataset<Row> lt_web;
		
		Dataset<Row> lt_all;
		
		gv_field = lv_field;
		
		go_select = new cl_seleciona();		
		
		lt_ips = lt_data.select(lv_field).distinct();
		
		//cl_util.m_show_dataset(lt_ips, "IPS");
				
		//fazer select na tabela local do IP info, e separa os IPs que não encontrou local pesquisa			
		
		//fazer um limtador selecionar no maximo 1000 linhas para a função abaixo
		
		//select distinct
				
		go_select.m_conf_phoenix(gc_table, gv_session);
		
		lt_ips_hb = go_select.m_select_IpInfo(lt_ips,lv_field);
		
		lt_ips_hb = lt_ips_hb.drop(gc_ts)
							 .drop(gv_field);
		
		System.out.println("\n TABBB"+lt_ips_hb);
		
		cl_util.m_show_dataset(lt_ips_hb, "TABLE");
		
		lt_ips_nf = m_ip_NotFound(lt_data, lt_ips_hb);
		
		System.out.println("\nAntes do COUNT");
		
		long lv_nf = 0;
		
		try {
			lv_nf = lt_ips_nf.count();
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		if(lt_ips_hb.count() == 0){
			
			lv_nf = 1;
			lt_ips_nf = lt_ips;
			
		}
				
		if(lv_nf > 0) { //VALIDAR ESSE IF o ELSE ta OK
			
			System.out.println("\nAntes do WEB");
			
			lt_web = m_search_WebService(lt_ips_nf.limit(1000)); //Máximo 1000 consultas no dia
			
			cl_util.m_show_dataset(lt_ips_hb, "TABLE");
			
			cl_util.m_show_dataset(lt_web, "WEB");			
			
			//Não precisa do UNION após inserir na tabela ele atualiza a TB. Salvar apenas no final do processo entao
			lt_all = lt_ips_hb;///lt_ips_hb.union(lt_web); // A ordem das colunas de LT_WEB tem de ser igual a LT_IPS_HB
			
			cl_util.m_show_dataset(lt_all, "UNION");
			
			lt_data = m_join_IpInfo(lt_data, lt_all);
			
			/*lt_all = lt_all.withColumn(gc_ts, functions.lit(gv_stamp));
			
			cl_util.m_save_log(lt_all, gc_table);
			
			cl_util.m_show_dataset(lt_ips_hb, "Após saveTABLE");*/
			
		}else {
		
			lt_data = m_join_IpInfo(lt_data, lt_ips_hb);
			
		}
				
		cl_util.m_show_dataset(lt_data, "FINAL");
	
		return lt_data;
		
	}
	
	public Dataset<Row> m_ip_NotFound(Dataset<Row> lt_data,Dataset<Row> lt_hb) {
		
		Dataset<Row> lt_res = null;
				
		try {
			
			lt_res = lt_data.join(lt_hb, col(gv_field).notEqual(gc_ip), "inner" )
					.select(gv_field);
	
			cl_util.m_show_dataset(lt_res, "Not Found");
			
		} catch (Exception e) {
			// TODO: handle exception
		}	
				
		return lt_res;
	}
	
	public Dataset<Row> m_join_IpInfo(Dataset<Row> lt_data, Dataset<Row> lt_ip){
		
		Dataset<Row> lt_res;
				
		lt_res = lt_data.join(lt_ip, lt_data.col(gv_field).equalTo(lt_ip.col(gc_ip)));
							
		cl_util.m_show_dataset(lt_res, "JOIN");
		
		return lt_res;
	}
	
	public Dataset<Row> m_search_WebService(Dataset<Row> lt_data){

		final String lc_token = "dc695e943d23f0"; 
		
		Dataset<Row> lt_res;
		
		Dataset<Row> lt_ip;
				
		lt_ip = lt_data.select(gv_field).distinct();
		
		Dataset<cl_IpInfo> lt_IpInfo = lt_ip.map( row->{ 
			
			cl_IpInfo lo_ip = new cl_IpInfo();	
								
			IPInfo ipInfo = IPInfo.builder().setToken(lc_token).build();
									
			IPResponse response = ipInfo.lookupIP(row.getString(0));
	            
	        //System.out.println("ALL:"+response.toString());
			
	        lo_ip.setIp(row.getString(0));
	        
	        lo_ip.setHostname(response.getHostname());
            
	        lo_ip.setCity(response.getCity());
            
	        lo_ip.setRegion(response.getRegion());
            
            lo_ip.setCountry(response.getCountryCode());
            
            lo_ip.setOrg(response.getOrg());
            
            lo_ip.setLatitude(Double.parseDouble(response.getLatitude()));
            
            lo_ip.setLongitude(Double.parseDouble(response.getLongitude()));
					
			return lo_ip;
	
		},Encoders.bean(cl_IpInfo.class));
		
		lt_res = lt_IpInfo.toDF();
		
		lt_res = lt_res.withColumn(gc_ts, functions.lit(gv_stamp));
		
		cl_util.m_save_log(lt_res, gc_table);
		
		lt_res = lt_res.select("ip"			,          
						       "hostname"   , 
						       "city"       , 
						       "region"  	,
						       "country"	,						       	
						       "org"        , 
						       "latitude"   , 
						       "longitude"  ); 
			
		return lt_res;	
		
	}

}
