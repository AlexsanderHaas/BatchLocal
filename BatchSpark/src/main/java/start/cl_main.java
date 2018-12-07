package start;

import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class cl_main {
	
	//---------CONSTANTES---------//
	
	final static String gc_table = "JSON00";
	
	final static String gc_conn_ip = "CONN_IP1";
	
	final static String gc_stamp = "2018-12-02 21:57:00.000"; //por aqui ele considera o GMT -2 e no SQL no CMD é sem GMT
	
	//---------ATRIBUTOS---------//
	
	private long gv_stamp;
	
	private Date gv_time = new Date();
	
	private static int gv_submit = 0; //1=Cluster 
	
	private static int gv_batch = 6;
	
	private Dataset<Row> gt_data;
	
	private static SparkConf gv_conf;
    
	private static SparkContext gv_context;
	                        
	private static SparkSession gv_session;
	
	//---------ATRIBUTOS-CLASSES---------//
	
	private static cl_main gv_main;
	
	private cl_processa go_processa;
	
	private cl_seleciona go_select;
	
	private cl_get_results go_results;
	
	private cl_util go_util;
	
	public static void main(String[] args) throws AnalysisException {
		
		gv_main = new cl_main();
		
		gv_main.m_start();

	}
	
	public void m_start() throws AnalysisException {
		
		m_conf_spark();
		
		gv_stamp = gv_time.getTime();	//Stamp do inicio do processamento
		
		go_select = new cl_seleciona();
		
		go_processa = new cl_processa(gc_stamp, gv_stamp);							
		
		switch(gv_batch){

		case 1: //SALVA todos os dados em CSV CONN, DNS e HTTP
			
			go_select.m_conf_phoenix(gc_table, "GERAL", gv_session);
			
			gt_data = go_select.m_seleciona(gc_stamp);
	
			if(gt_data.count() > 0) {
			
				go_processa.m_get_all(gt_data);
	
			}
			
			break;
		
		case 2: // Seleciona apenas o CONN e processa ORIG e RESP salvando os dados na tabela
		
			go_select.m_conf_phoenix(gc_table, "Conn_ORIG_RESP", gv_session);
			
			gt_data = go_select.m_seleciona_conn(gc_stamp);
			
			cl_util.m_show_dataset(gt_data, "Totais de CONN:");
			
			go_processa.m_process_orig(gt_data, gv_stamp);
			
			go_processa.m_process_resp(gt_data, gv_stamp);		
			
			break;
		
		case 3: //Seleciona os resultados da opção 2 que foram salvos nas tabelas
			
			go_results = new cl_get_results();
			
			go_results.m_start(gv_session);
			
			break;
					
		case 4: //Seleciona os dados CONN processando os totais
			
			go_select.m_conf_phoenix(gc_table, "TotaisConn", gv_session);
			
			gt_data = go_select.m_seleciona_conn(gc_stamp);
			
			go_processa.m_process_totais(gt_data);
			
			break;
						
		case 5: //Processa e salva as Análises na tabela
					
			go_select.m_conf_phoenix(gc_table, "TotaisConn", gv_session);
			
			gt_data = go_select.m_seleciona(gc_stamp);
			
			go_processa.m_start_analyzes(gt_data);			
			
			break;
			
		case 6:
			
			Dataset<Row> lt_res;
			
			cl_kmeans lo_kmeans = new cl_kmeans();
			
			go_select.m_conf_phoenix(gc_table, "K-means", gv_session);
			
			gt_data = go_select.m_seleciona(gc_stamp);
			
			lt_res = lo_kmeans.m_normaliza_dados(gt_data);
			
			//lo_kmeans.m_kmeans(gt_data, gv_session);
			
			//lo_kmeans.m_ddos_kmeans(lt_res, gv_session);
			
			break;

		}		
				
	}
	
	public static void m_conf_spark(){
		
		if(gv_submit == 0) {			
			gv_conf = new SparkConf().setMaster("local[4]").setAppName("SelectLog");
		}else {
			gv_conf = new SparkConf().setAppName("ProcessaIp");//se for executar no submit
		}
				
		gv_context = new SparkContext(gv_conf);
		
		gv_session = new SparkSession(gv_context);		
		
		Logger.getRootLogger().setLevel(Level.ERROR);
	}

}
