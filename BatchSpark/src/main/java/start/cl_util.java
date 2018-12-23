package start;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class cl_util {
	
	final static String gc_zkurl = cl_seleciona.gc_zkurl; //"localhost:2181";
	
	final static String gc_path_r = "/home/user/Documentos/batch_spark_r/";
	
	static long gv_ini;
	
	public static void m_show_dataset(Dataset<Row> lt_data, String lv_desc) {
		
		try {
			
			System.out.println("\n" + lv_desc + "\t" + lt_data.count());
						
			lt_data.printSchema();
			
			lt_data.show(5);
			
		} catch (Exception e) {
			System.out.println("\nProblema: Classe CL_UTIL Método SHOW_DATASET. Chamada do método:" + lv_desc + "\t" + e);
		}
								
	}
	
	public static void m_save_csv(Dataset<Row> lv_data, String lv_dir){
		
		String lv_path = gc_path_r + lv_dir;
		
		lv_data.coalesce(1) //cria apenas um arquivo
	      .write()
	      .option("header", "true")
	      .mode("overwrite") //substitui o arquivo de resultado pelo novo				     
	      .csv(lv_path);
		
	}
	
	public static void m_save_json(Dataset<Row> lv_data, String lv_dir){
		
		String lv_path = gc_path_r + lv_dir;
		
		lv_data.coalesce(1) //cria apenas um arquivo
	      .write()
	      .option("header", "true")
	      .mode("overwrite") //substitui o arquivo de resultado pelo novo				     
	      .json(lv_path);
		
	}
	
	public static void m_save_log(Dataset<Row> lt_data, String lt_table) {
		
		long lv_num = 0;			
		
		try {
			lv_num = lt_data.count();
		} catch (Exception e) {

		}				
		
		if(lv_num > 0) {
			
			lt_data.write()
				.format(cl_seleciona.gc_phoenix)
				.mode("overwrite")
				.option("table", lt_table)
				.option("zkUrl", cl_seleciona.gc_zkurl)
				.option("autocommit", "true")
				.save();
		}		
					
	}
	
	public static void m_time_start() {
		
		gv_ini = System.currentTimeMillis();  
		
	}
	
	public static void m_time_end() {
		
		long lv_f = ( System.currentTimeMillis() - gv_ini ) / 1000;
		
		System.out.println("\n A função foi executada em:\t" + lv_f +" Segundos");
		
	}	
	
}
