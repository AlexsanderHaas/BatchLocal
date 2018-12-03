package start;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;

import java.util.HashMap;
import java.util.Map;

public class cl_get_results {
	
	final static String gc_table = "CONN_IP1";
	
	final static String gc_zkurl = "localhost:2181";
	
	
	//---------ATRIBUTOS---------//
	
	private static Map<String, String> gv_phoenix;

	private static SparkSession gv_session;

	private Dataset<Row> gt_data;
	
	public void m_start(SparkSession lv_session) throws AnalysisException {		
		
		gv_session = lv_session;
		
		m_conecta_phoenix();
		
		m_resp_conn();
		
	}
	
	public static void m_conecta_phoenix(){
		
		gv_phoenix = new HashMap<String, String>();
		
		gv_phoenix.put("zkUrl", gc_zkurl);
		gv_phoenix.put("hbase.zookeeper.quorum", "master");
		gv_phoenix.put("table", gc_table);
				
	}
	
	public void m_resp_conn() {
		
		Dataset<Row> lt_orig;
		Dataset<Row> lt_resp;
		
		gt_data = gv_session
			      .sqlContext()
			      .read()
			      .format("org.apache.phoenix.spark")
			      .options(gv_phoenix)							   
			      .load();		      			      
			      //.filter(col("TS_CODE").gt(gc_stamp));
			      //.filter(col("ID_ORIG_H").isNotNull());
				  //.sort(col("DURATION ").desc());					   
		
		cl_util.m_show_dataset(gt_data, "Totais de CONN:");
		
		lt_orig = gt_data.filter(col("ID_ORIG_H").isNotNull());					
				
		cl_util.m_save_csv(lt_orig, "CONN_ORIG" );
		
		lt_resp = gt_data.filter(col("ID_RESP_H").isNotNull());			
		
		cl_util.m_save_csv(lt_resp, "CONN_RESP" );
		
	}
		
}














