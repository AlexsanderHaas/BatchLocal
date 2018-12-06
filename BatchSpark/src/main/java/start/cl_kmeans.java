package start;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.Minute;
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum;

import scala.Array;

import org.apache.spark.sql.Column;

public class cl_kmeans {

	//------------------Colunas------------------------//
	final static String gc_ts 			= "TS";
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
	
	final static String gc_count   		= "COUNT";
	
	final static String lc_duration     = "sum(DURATION)"; 
	final static String lc_orig_pkts    = "sum(ORIG_PKTS)";
	final static String lc_orig_bytes   = "sum(ORIG_BYTES)";
	final static String lc_resp_pkts	= "sum(RESP_PKTS)";
	final static String lc_resp_bytes   = "sum(RESP_BYTES)";	
	
	
	Dataset<Row> gt_data;
	
	public Dataset<Row> m_normaliza_dados(Dataset<Row> lt_data) {	
						
		Dataset<Row> lt_res;
		
		Dataset<Row> lt_count;
		
		cl_util.m_time_start();				
		
		lt_res = lt_data.select( gc_orig_h  ,
				                 gc_resp_h  ,
				                 gc_resp_p  ,
				                 gc_proto   ,
				                 gc_service ,
				                 gc_ts,
				                 gc_duration,
				                 gc_orig_pkts, 
				                 gc_orig_bytes,
				                 gc_resp_pkts,	
				                 gc_resp_bytes )	   
					   .withColumn(gc_ts, date_format(col(gc_ts), "dd.MM.yyyy HH:mm"))					   
					   .groupBy( col(gc_orig_h),
							     col(gc_resp_h), 
							     col(gc_resp_p), 
							     col(gc_proto),  
		                         col(gc_service),
		                         col(gc_ts))
					   .agg(sum(gc_duration), 
							sum(gc_orig_pkts), 
							sum(gc_orig_bytes),
							sum(gc_resp_pkts),	
							sum(gc_resp_bytes),
							count("*"))
					   .withColumnRenamed("count(1)", gc_count)
					   .withColumnRenamed(lc_duration, gc_duration)
					   .withColumnRenamed(lc_orig_pkts, gc_orig_pkts)
					   .withColumnRenamed(lc_orig_bytes, gc_orig_bytes)
					   .withColumnRenamed(lc_resp_pkts, gc_resp_pkts)
					   .withColumnRenamed(lc_resp_bytes, gc_resp_bytes);
					   
		
		lt_res = lt_res.sort(gc_orig_h,
							 gc_ts,
							 gc_count);
		
		cl_util.m_show_dataset(lt_res, "1) Normaliza Kmeans");									
		
		lt_res.filter(col(gc_resp_h).equalTo("205.174.165.73"))
			  .groupBy(gc_orig_h)
			  .count()
			  .sort(col("count").desc())
			  .show();;
		
		//cl_util.m_save_csv(lt_res, "DDoS_HTTP_NEW");			
		
		cl_util.m_time_end();
		
		return lt_res;

		
	}
	
	public void m_ddos_kmeans(Dataset<Row> lt_data, SparkSession lv_session) {
				
		VectorAssembler lv_assembler = new VectorAssembler()
										.setInputCols(new String[]{"COUNT", "ORIG_BYTES", "RESP_BYTES"}) //era orig_ip_bytes
										.setOutputCol("features");
		
		Dataset<Row> lt_http = lt_data.filter(col(cl_processa.gc_service).equalTo("http"))
									  .filter(col("ORIG_BYTES").isNotNull())
									  .filter(col("RESP_BYTES").isNotNull())
									  .sort("ORIG_BYTES");
		
		cl_util.m_show_dataset(lt_http, "Dados http:");
		
		Dataset<Row> lv_vector = lv_assembler.transform(lt_http);	
		
		 // Trains a k-means model.
	    KMeans kmeans = new KMeans().setK(3);//.setSeed(1L);
	    
	    KMeansModel model = kmeans.fit(lv_vector);
	
	    // Evaluate clustering by computing Within Set Sum of Squared Errors.
	    double WSSSE = model.computeCost(lv_vector);
	    
	    System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
	
	    // Shows the result.
	    Vector[] centers = model.clusterCenters();
	    
	    System.out.println("Cluster Centers: ");
	    
	    for (Vector center: centers) {
	    	
	      System.out.println(center);
	      
	    }
	    
	    // Make predictions
	    //Dataset<Row> predictions = model.transform(lt_sum);
	
	    // Evaluate clustering by computing Silhouette score
	    /*ClusteringEvaluator evaluator = new ClusteringEvaluator();
	
	    double silhouette = evaluator.evaluate(predictions);
	    System.out.println("Silhouette with squared euclidean distance = " + silhouette);*/
	
	    Dataset<Row> lt_res = model.transform(lv_vector).sort(col("prediction").desc());	    	    
	    	        	    
	    //cl_util.m_save_csv(lt_res, "DDoS-Kmeans");
	    
	    cl_util.m_show_dataset(lt_res, "Kmeans DDdos");
	    
	    
		
	}
	
	public void m_kmeans(Dataset<Row> lt_data, SparkSession lv_session) {
		
		Dataset<Row> lt_sum;
		
		lt_sum = lt_data.filter(col("SUM(DURATION)").isNotNull()) //não pode ter valores nulos
						.filter(col("SUM(ORIG_PKTS)").isNotNull())
						.filter(col("SUM(ORIG_BYTES)").isNotNull());
				
		/*lt_sum.printSchema();
		lt_sum.show();*/
		
		VectorAssembler lv_assembler = new VectorAssembler()
										.setInputCols(new String[]{"SUM(DURATION)", "SUM(ORIG_PKTS)", "SUM(ORIG_BYTES)"})
										.setOutputCol("features");
		
		
		
		Dataset<Row> lv_vector = lv_assembler.transform(lt_sum);
		Dataset<Row> lv_vector1 = lv_assembler.transform(lt_sum);
		
		 // Trains a k-means model.
	    KMeans kmeans = new KMeans().setK(2);//.setSeed(1L);
	    
	    KMeansModel model = kmeans.fit(lv_vector);
	
	    // Evaluate clustering by computing Within Set Sum of Squared Errors.
	    double WSSSE = model.computeCost(lv_vector);
	    
	    System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
	
	    // Shows the result.
	    Vector[] centers = model.clusterCenters();
	    
	    System.out.println("Cluster Centers: ");
	    
	    for (Vector center: centers) {
	    	
	      System.out.println(center);
	      
	    }
	    
	    // Make predictions
	    //Dataset<Row> predictions = model.transform(lt_sum);
	
	    // Evaluate clustering by computing Silhouette score
	    /*ClusteringEvaluator evaluator = new ClusteringEvaluator();
	
	    double silhouette = evaluator.evaluate(predictions);
	    System.out.println("Silhouette with squared euclidean distance = " + silhouette);*/
	
	    Dataset<Row> lv_res = model.transform(lv_vector1).sort(col("prediction").desc());	    	    
	    
	    System.out.println("Total: "+lv_res.count());
	    	    
	    lv_res.printSchema();
	    
	    lv_res.show(100);
		
	}

}
