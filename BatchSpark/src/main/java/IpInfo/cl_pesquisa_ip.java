package IpInfo;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

import io.ipinfo.api.IPInfo;
import io.ipinfo.api.model.IPResponse;
import start.cl_main;
import start.cl_seleciona;
import start.cl_util;

public class cl_pesquisa_ip {
	
	//---------CONSTANTES---------//
	
	final String gc_token = "dc695e943d23f0";
	
	final String gc_table = "IP_INFO";
	
	//---------ATRIBUTOS---------//
	
	private SparkSession gv_session;
	
	private cl_seleciona go_select;
	
	private String gv_field;
	
	//---------METODOS---------//
	
	public cl_pesquisa_ip(SparkSession lv_session) {
		
		gv_session = lv_session;
		
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
				
		//fazer select na tabela local do IP info, e separa os IPs que não encontrou local pesquisa			
		
		//fazer um limtador selecionar no maximo 1000 linhas para a função abaixo
		
		//select distinct
				
		go_select.m_conf_phoenix(gc_table, gv_session);
		
		lt_ips_hb = go_select.m_select_IpInfo(lt_ips,lv_field);
		
		cl_util.m_show_dataset(lt_ips_hb, "TABLE");
		
		lt_ips_nf = m_ip_NotFound(lt_data, lt_ips_hb);
		
		System.out.println("\nAntes do COUNT");
		
		if(lt_ips_nf.count() > 0) { //VALIDAR ESSE IF o ELSE ta OK
			
			lt_web = m_search_WebService(lt_ips_nf.limit(1000)); //Máximo 1000 consultas no dia
			
			lt_all = lt_ips.union(lt_web);
			
			cl_util.m_show_dataset(lt_all, "UNION");
			
			m_join_IpInfo(lt_data, lt_all);
			
		}else {
		
			lt_data = m_join_IpInfo(lt_data, lt_ips_hb);
			
		}
				
		cl_util.m_show_dataset(lt_data, "FINAL");
	
		return lt_data;
		
	}
	
	public Dataset<Row> m_ip_NotFound(Dataset<Row> lt_data,Dataset<Row> lt_hb) {
		
		Dataset<Row> lt_res;
		
		String lv_cond = "dt."+ gv_field + " <> hb.IP";
		
		lt_res = lt_data.join(lt_hb, col(gv_field).notEqual("IP"), "left_anti" )
						.select(gv_field);
		
		cl_util.m_show_dataset(lt_res, "Not Found");
		
		return lt_res;
	}
	
	public Dataset<Row> m_join_IpInfo(Dataset<Row> lt_data, Dataset<Row> lt_ip){
		
		Dataset<Row> lt_res;
		
		String lv_cond = "dt."+ gv_field + " = tb.ip";
		
		lt_res = lt_data.join(lt_ip, lt_data.col(gv_field).equalTo(lt_ip.col("IP")));
							
		cl_util.m_show_dataset(lt_res, "JOIN");
		
		return lt_res;
	}
	
	public Dataset<Row> m_search_WebService(Dataset<Row> lt_data){

		Dataset<Row> lt_res;
		
		String lv_col[] = new String[8];
		
		Dataset<cl_IpInfo> lt_IpInfo = lt_data.map( row->{ 
			
			cl_IpInfo lo_ip = new cl_IpInfo();	
									
			IPInfo ipInfo = IPInfo.builder().setToken(gc_token).build();
			
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
		
						
		lv_col[0] = "ip";					
		lv_col[1] = "hostname";
		lv_col[2] = "city";
		lv_col[3] = "country";		
		lv_col[4] = "region";  		
		lv_col[5] = "org";     
		lv_col[6] = "latitude";
		lv_col[7] = "longitude";
				
		lt_res = lt_IpInfo.toDF(lv_col);
		
		lt_res.withColumn("TS_CODE", functions.lit(cl_main.gv_stamp));
		
		cl_util.m_show_dataset(lt_res, "TO DF IPS");
		
		cl_util.m_save_log(lt_res, gc_table);
				
		return lt_res;
	}

}
