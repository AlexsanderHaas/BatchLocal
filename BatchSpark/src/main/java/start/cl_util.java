package start;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class cl_util {
	
	final static String gc_zkurl = "localhost:2181";
	
	final static String gc_path_r = "/home/user/Documentos/batch_spark/";
	
	public static void m_show_dataset(Dataset<Row> lt_data, String lv_desc) {
		
		System.out.println("\n" + lv_desc + "\t" + lt_data.count());
		
		//lt_data.printSchema();
		
		lt_data.show(5);
						
	}
	
	public static void m_save_csv(Dataset<Row> lv_data, String lv_dir){
		
		String lv_path = gc_path_r + lv_dir;
		
		lv_data.coalesce(1) //cria apenas um arquivo
	      .write()
	      .option("header", "true")
	      .mode("overwrite") //substitui o arquivo de resultado pelo novo				     
	      .csv(lv_path);
		
	}
	
	public static void m_save_log(Dataset<Row> lt_data, String lt_table) {
		
		long lv_num = lt_data.count();			
			
		if(lv_num > 0) {
			
			//System.out.println("\nVai SALVAR Conexões - " + lv_num );
			
			lt_data.write()
				.format("org.apache.phoenix.spark")
				.mode("overwrite")
				.option("table", lt_table)
				.option("zkUrl", gc_zkurl)
				.option("autocommit", "true")
				.save();
		}		
					
	}
	
}
