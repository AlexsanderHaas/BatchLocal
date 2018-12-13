package IpInfo;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import io.ipinfo.api.IPInfo;
import io.ipinfo.api.model.IPResponse;
import start.cl_seleciona;

public class cl_pesquisa_ip {
	
	final String gc_token = "dc695e943d23f0";
	
	final String gc_table = "IP_INFO";
		
	public Dataset<Row>  m_processa_ip(Dataset<Row> lt_data, String lv_field) {
		
		Dataset<Row> lt_ips;
		
		lt_ips = lt_data.select(lv_field).distinct();
				
		//fazer select na tabela local do IP info, e separa os IPs que não encontrou local pesquisa			
		
		//fazer um limtador selecionar no maximo 1000 linhas para a função abaixo
		
		//select distinct
		
		Dataset<cl_IpInfo> lt_IpInfo = lt_ips.map( row->{ 
			
						cl_IpInfo lo_ip = new cl_IpInfo();	
												
						IPInfo ipInfo = IPInfo.builder().setToken(gc_token).build();
						
						IPResponse response = ipInfo.lookupIP(row.getString(0));
				            
				        System.out.println("ALL:"+response.toString());
						
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
		
		String lv_col[] = new String[8];
		
		lv_col[0] = "hostname";
		lv_col[1] = "city";
		lv_col[2] = "country";		
		lv_col[3] = "region";  		
		lv_col[4] = "org";     
		lv_col[5] = "latitude";
		lv_col[6] = "longitude";
		lv_col[7] = "ip";
		
		Dataset<Row>lt_res = lt_IpInfo.toDF(lv_col);
				
		return lt_data;
		
	}
	
	public void m_save_IpInfo(Dataset<cl_IpInfo> lt_data) {
		
		long lv_num = lt_data.count();			
		
		if(lv_num > 0) {
		
			lt_data.write()
			       .format(cl_seleciona.gc_phoenix)
			       .mode("overwrite")
			       .option("table", gc_table)
			       .option("zkUrl", cl_seleciona.gc_zkurl)
			       .option("autocommit", "true")
			       .save();
	
		}
	}
}
