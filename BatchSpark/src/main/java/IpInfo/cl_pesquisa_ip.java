package IpInfo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;

import static org.apache.spark.sql.functions.col;

import io.ipinfo.api.IPInfo;
import io.ipinfo.api.model.IPResponse;
import start.cl_seleciona;
import start.cl_util;

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
	
	private Dataset<Row> gt_ip_web;
	
	//---------METODOS---------//
	
	public cl_pesquisa_ip(SparkSession lv_session, long lv_stamp) {
		
		gv_session = lv_session;
		
		gv_stamp = lv_stamp;
		
	}
	
	public Dataset<Row> m_processa_ip(Dataset<Row> lt_data, String lv_field) {
		
		Dataset<Row> lt_ips;
		
		Dataset<Row> lt_ips_hb = null;
		
		Dataset<Row> lt_ips_nf;
		
		Dataset<Row> lt_union;
		
		Dataset<Row> lt_all = null;
		
		gv_field = lv_field;
		
		go_select = new cl_seleciona();		
		
		go_select.m_conf_phoenix(gc_table, gv_session);
		
		int lv_free = go_select.m_Limit_IpInfo(); //Verifica limite diario
		
		System.out.println("\nConsultas disponiveis no dia: " + lv_free);
		
		lt_ips = lt_data.select(lv_field).distinct().persist(StorageLevel.MEMORY_ONLY());
				
		cl_util.m_show_dataset(lt_ips, "Número de IPS DISTINCT: ");						
		
		lt_ips_hb = go_select.m_select_IpInfo(lt_ips,lv_field);
		
		lt_ips_hb = lt_ips_hb.drop(gc_ts)
							 .drop(gv_field)
							 .persist(StorageLevel.MEMORY_ONLY());
		
		cl_util.m_show_dataset(lt_ips_hb, "HBase:IPs IpInfo: ");	
		
		lt_ips_nf = m_ip_NotFound(lt_ips, lt_ips_hb);
		
		long lv_nf = 0;
		
		try {
			lv_nf = lt_ips_nf.count();
		} catch (Exception e) {	
			lv_nf = 1;
			lt_ips_nf = lt_ips;	
		}
			
		if(lv_nf > 0 && lv_free > 0) { //Se não encontrou tudo na tabela pesquisa no Web Service os que faltam
			
			//System.out.println("\n Pesquisa no WEB Service: " + lv_nf);
			m_search_WebService(lt_ips_nf.limit(lv_free)); //Máximo 1000 consultas no dia
			
			try {
				lt_union = lt_ips_hb.union(gt_ip_web.drop(gc_ts));
			} catch (Exception e) {				
				lt_union = lt_ips_hb;
				System.out.println("\n Erro UNION: Sem dados: "+e);	
			}
			
			try {
				lv_nf = lt_union.count();			
			} catch (Exception e) { //Se não encontrar nenhum IPretorna os mesmos dados
				System.out.println("\n Erro não encontrou na WEB nem no banco: " + e);
				return lt_data;
			}
			
			if(lv_nf == 0) {
				System.out.println("\n Não encontrou na WEB nem no banco: " + lv_nf);
				return lt_data;
			}
								
			lt_all = m_join_IpInfo(lt_data, lt_union);		
			
			cl_util.m_save_log(gt_ip_web, gc_table); 
			//Pois se salvar logo após a consulta, 
			//ocorre EXCEPTION de concorrencia, pois nem salvou na tabela e ja consulta novamente. Assim salva apenas no final do processamento.
			
		}else { //Agrupa as colunas do IPInfo
		
			lt_all = m_join_IpInfo(lt_data, lt_ips_hb);
			
		}		
		
		return lt_all;
		
	}
	
	public Dataset<Row> m_ip_NotFound(Dataset<Row> lt_data,Dataset<Row> lt_hb) {
		
		Dataset<Row> lt_res = null;
				
		try {
			
			lt_res = lt_data.join(lt_hb, col(gv_field).equalTo(col(gc_ip)),"left_anti");
					
			lt_res = lt_res.select(gv_field).distinct();
			
			cl_util.m_show_dataset(lt_res, "Not Found IPInfo IN HBase");
			
		} catch (Exception e) {
			System.out.println("\n Método NotFound IPInfo HBase:" + "\t" + e);
		}	
				
		return lt_res;
	}
	
	public Dataset<Row> m_join_IpInfo(Dataset<Row> lt_data, Dataset<Row> lt_ip){
		
		Dataset<Row> lt_res;
				
		lt_res = lt_data.join(lt_ip, lt_data.col(gv_field).equalTo(lt_ip.col(gc_ip)),"left_outer");
							
		cl_util.m_show_dataset(lt_res, "JOIN: Agrupou o IPInfo com os dados do banco:");
		
		return lt_res.drop(gc_ip);
	}
	
	public void m_search_WebService(Dataset<Row> lt_data){

		final String lc_token = "dc695e943d23f0"; 
		
		Dataset<Row> lt_res;
		
		Dataset<Row> lt_ip;
				
		lt_ip = lt_data.select(gv_field).distinct().limit(5);
				
		Dataset<cl_IpInfo> lt_IpInfo = lt_ip.map( row->{ 
			
			//System.out.println("\n IP:"+row.getString(0));
			
			cl_IpInfo lo_ip = new cl_IpInfo();	
			
			IPResponse response = null;
			
			IPInfo ipInfo = IPInfo.builder().setToken(lc_token).build();
			
			try {
				response = ipInfo.lookupIP(row.getString(0));
			} catch (Exception e) {
				System.out.println("Erro Web Service:"+e);
			}					
			   
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
				
		try {
			
			lt_res = lt_IpInfo.toDF().persist(StorageLevel.MEMORY_ONLY());

			lt_res = lt_res.withColumn(gc_ts, functions.lit(gv_stamp));
			
			cl_util.m_show_dataset(lt_res, "Web Service IPInfo: ");
			
			gt_ip_web = lt_res;
			
		} catch (Exception e) {
			System.out.println("\n Erro WEB nao consultou: " + e);
		}			
						
	}

}
