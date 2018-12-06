package start;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.sum;

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
import org.apache.spark.sql.Column;

public class cl_kmeans {

	//------------------Colunas------------------------//
	
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
	
	Dataset<Row> gt_data;
	
	public Dataset<Row> m_normaliza_dados(Dataset<Row> lt_data) {	
		
		final String lc_table = "LOG"; 
		
		final String lc_ts = "TS";
		
		String lc_v = ", ";
		
		String lv_grp =  cl_processa.gc_orig_h + lc_v +
						 cl_processa.gc_resp_h + lc_v +
						 cl_processa.gc_resp_p + lc_v +
						 cl_processa.gc_proto  + lc_v +
						 cl_processa.gc_service;
		
		String lv_cols = lv_grp + ", date_format(TS, 'dd.MM.yyyy HH:mm') AS TS" + ", COUNT(*) AS COUNT, ";
		
		lv_grp = lv_grp + lc_v + lc_ts;
		
		String lv_sum = cl_processa.lv_sum + lc_v +
						"SUM(ORIG_IP_BYTES) AS ORIG_IP_BYTES , " +
						"SUM(RESP_IP_BYTES) AS RESP_IP_BYTES ";
		
		String lv_sql = "SELECT " +
						lv_cols    +
						lv_sum    +
						"FROM "   + lc_table +
						" GROUP BY "+ lv_grp;
		
		Dataset<Row> lt_res;
		
		cl_util.m_time_start();
		
		lt_data.createOrReplaceTempView(lc_table); //cria uma tabela temporaria, para acessar via SQL				
			
		System.out.println("SQL: "+lv_sql);
				
		lt_res = lt_data.sparkSession()
						.sql(lv_sql);//.withColumn("TS", date_format(col("TS"), "dd.MM.yyyy HH:mm"));;	
		
		cl_util.m_show_dataset(lt_res, "1) Normaliza Kmeans");				
				
		lt_res = lt_res.select("*")					   
					   //.withColumn("TS", date_format(col("TS"), "dd.MM.yyyy HH:mm"))					   
					   .groupBy( col(cl_processa.gc_orig_h),
							     col(cl_processa.gc_resp_h), 
							     col(cl_processa.gc_resp_p), 
							     col(cl_processa.gc_proto),  
		                         col(cl_processa.gc_service),
		                         col(lc_ts))
					   .sum(cl_processa.gc_duration,  
							cl_processa.gc_orig_pkts, 
							cl_processa.gc_orig_bytes,
							cl_processa.gc_resp_pkts,	
							cl_processa.gc_resp_bytes, "COUNT");
		
		
		
		
					   //.count();
				
	/*	lt_res = lt_data.select(col("ID_ORIG_H"),sum("DURATION"))					   
				        .withColumn("TS", date_format(col("TS"), "dd.MM.yyyy HH:mm"))
				        .groupBy(col("ID_ORIG_H"),col("TS")).count();*/					   
				        //.groupBy( col(cl_processa.gc_orig_h), col("sum(DURATION)"));					   			
				   //.count();
		
		//lt_res = lt_res.sort(col("COUNT").desc());
		
		cl_util.m_show_dataset(lt_res, "2) Normaliza Kmeans");									
		
		cl_util.m_save_csv(lt_res, "DDoS_HTTP_TG");			
		
		cl_util.m_time_end();
		
		return lt_res;

		
	}
	
	public void m_ddos_kmeans(Dataset<Row> lt_data, SparkSession lv_session) {
				
		VectorAssembler lv_assembler = new VectorAssembler()
										.setInputCols(new String[]{"COUNT", "ORIG_IP_BYTES", "RESP_IP_BYTES"})
										.setOutputCol("features");
		
		Dataset<Row> lt_http = lt_data.filter(col(cl_processa.gc_service).equalTo("http"))
									  .filter(col("ORIG_IP_BYTES").isNotNull())
									  .filter(col("RESP_IP_BYTES").isNotNull())
									  .sort("ORIG_IP_BYTES");
		
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
