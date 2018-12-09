package start;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Column;

public class cl_kmeans {
	
	//---------CONSTANTES---------//
	
	final String gc_kmeans_ddos = cl_main.gc_kmeans_ddos;
	
	final  String gc_format = "dd.MM.yyyy HH:mm";	
	
	//------------------Colunas------------------------//
	
	final static String gc_tipo 		= "TIPO";
	final static String gc_ts_code 		= "TS_CODE";
	final static String gc_ts_filtro	= "TS_FILTRO";
	final static String gc_rowid		= "ROW_ID";
	       
	final static String gc_ts 			= "TS";
	final static String gc_proto 		= "PROTO";
	final static String gc_service		= "SERVICE";
	final static String gc_orig_h		= "ID_ORIG_H";
	final static String gc_orig_p		= "ID_ORIG_P";
	final static String gc_resp_h		= "ID_RESP_H";
	final static String gc_resp_p		= "ID_RESP_P";
		  
	final static String gc_duration     = "DURATION";
	final static String gc_o_bytes      = "ORIG_IP_BYTES";
	final static String gc_r_bytes      = "RESP_IP_BYTES";
	final static String gc_orig_pkts    = "ORIG_PKTS";
	final static String gc_orig_bytes   = "ORIG_BYTES";
	final static String gc_resp_pkts  	= "RESP_PKTS";
	final static String gc_resp_bytes   = "RESP_BYTES";		
	      
	final static String gc_count   	    = "COUNT";
	                                    
	final static String lc_duration     = "sum(DURATION)";
	final static String lc_o_bytes      = "sum(ORIG_IP_BYTES)"; 
	final static String lc_r_bytes      = "sum(RESP_IP_BYTES)"; 
	final static String lc_orig_pkts    = "sum(ORIG_PKTS)";
	final static String lc_orig_bytes   = "sum(ORIG_BYTES)";
	final static String lc_resp_pkts 	= "sum(RESP_PKTS)";
	final static String lc_resp_bytes   = "sum(RESP_BYTES)";	
	
	//---------ATRIBUTOS---------//	
	
	private String gv_tipo;	
		
	private long gv_stamp_filtro; //Filtro da seleção de dados
	
	private long gv_stamp; //Stamp do inicio da execução
	
	public cl_kmeans(String lv_filtro, long lv_stamp){
		
		java.sql.Timestamp lv_ts = java.sql.Timestamp.valueOf( lv_filtro ) ;
		
		gv_stamp_filtro = lv_ts.getTime(); 
		
		gv_stamp = lv_stamp;
		
	}
	
	public void m_start_kmeans_ddos(SparkSession lv_session, Dataset<Row> lt_data, String lv_service) {
		
		Dataset<Row> lt_res;
		
		gv_tipo = lv_service; //Fazer filtro por PROTOCOLO TAMBÈM?
		
		lt_res = m_normaliza_analise_ddos(lt_data);
		
		m_ddos_kmeans(lt_res, lv_session);
		
	}

	public Dataset<Row> m_normaliza_analise_ddos(Dataset<Row> lt_data) {	
						
		Dataset<Row> lt_res;
			
		cl_util.m_time_start();				
		
		//Query que agrupa todas as portas de Origem Por IP e Minuto
		
		lt_res = lt_data.select( gc_orig_h  ,
								 gc_orig_p  ,
								 gc_resp_h  ,
				                 gc_resp_p  ,
				                 gc_proto   ,
				                 gc_service ,
				                 gc_ts,
				                 gc_duration,
				                 gc_o_bytes,
				                 gc_r_bytes,
				                 gc_orig_pkts, 
				                 gc_orig_bytes,
				                 gc_resp_pkts,	
				                 gc_resp_bytes )					   
					   .filter(col(gc_service).equalTo(gv_tipo))
					   .withColumn(gc_ts, date_format(col(gc_ts), gc_format))					   
					   //.withColumn(gc_ts, date_format(col(gc_ts), "dd.MM.yyyy HH")) Salvando por minuto na tabela depois consigo filtrar por HORA
					   .groupBy( col(gc_orig_h),							     
							     col(gc_resp_h), 
							     col(gc_resp_p), 
							     col(gc_proto),  
		                         col(gc_service),
		                         col(gc_ts))
					   .agg(sum(gc_duration),
							sum(gc_orig_pkts), 
							sum(gc_o_bytes),
							sum(gc_orig_bytes),
							sum(gc_resp_pkts),	
							sum(gc_r_bytes),
							sum(gc_resp_bytes),
							count("*"))
					   .withColumnRenamed("count(1)", gc_count)
					   .withColumnRenamed(lc_duration, gc_duration)
					   .withColumnRenamed(lc_orig_pkts, gc_orig_pkts)
					   .withColumnRenamed(lc_o_bytes, gc_o_bytes)
					   .withColumnRenamed(lc_orig_bytes, gc_orig_bytes) //Podem ser imprecisos de acordo com o BRO
					   .withColumnRenamed(lc_resp_pkts, gc_resp_pkts)
					   .withColumnRenamed(lc_r_bytes, gc_r_bytes)
					   .withColumnRenamed(lc_resp_bytes, gc_resp_bytes); //Podem ser imprecisos de acordo com o BRO				   
					   		
		lt_res = lt_res.sort(gc_orig_h,
							 gc_ts,
							 gc_count);
		
		lt_res = lt_res.withColumn(gc_ts_filtro, functions.lit(gv_stamp_filtro))						   
			           .withColumn(gc_ts_code, functions.lit(gv_stamp))
			           .withColumn(gc_rowid, functions.monotonically_increasing_id())
			           .withColumn(gc_ts, to_timestamp(col(gc_ts),gc_format)); //para salvar no banco coloca em timestamp novamente
		
		cl_util.m_show_dataset(lt_res, "1) Normaliza Kmeans");									
		
		//m_IpOrig_ForTime(lt_res);
		
		//cl_util.m_save_csv(lt_res, "ORIG_H_P");
		
		cl_util.m_time_end();
		
		return lt_res;
		
	}
	
	public void m_IpOrig_ForTime(Dataset<Row> lt_res) { //DDoS Attack
				
		Dataset<Row> lt_count;
		
		lt_count = lt_res.groupBy(gc_ts)
	                     .pivot(gc_orig_h)
						 //.pivot(gc_resp_h)
	                     .sum(gc_count)
	                     .sort(col(gc_ts));
	                     
		cl_util.m_show_dataset(lt_count, "1) IPS");

		cl_util.m_save_csv(lt_count, "GRF_ORIGHP");			
		
	}
	
	public void m_ddos_kmeans(Dataset<Row> lt_data, SparkSession lv_session) {
		
		final String lc_feat = "features";
		
		VectorAssembler lv_assembler = new VectorAssembler();
		
		int lv_k = 2;
		
	    String lv_centroid[] = new String[lv_k];
	    	    	    
		cl_util.m_time_start();
		
		//Executa o Kmeans com a contagem de IP Origem para IP Destino e mesma porta. Caracteristica de DDoS 
				
		lv_assembler.setInputCols(new String[]{ gc_count })
				    .setOutputCol(lc_feat);
									
		Dataset<Row> lv_vector = lv_assembler.transform(lt_data);	
		
		 // Trains a k-means model.
	    KMeans kmeans = new KMeans().setK(lv_k);//.setSeed(1L);
	    
	    KMeansModel model = kmeans.fit(lv_vector);
	
	    // Evaluate clustering by computing Within Set Sum of Squared Errors.
	    double WSSSE = model.computeCost(lv_vector);
	    
	    System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
	
	    // Shows the result.
	    Vector[] centers = model.clusterCenters();
	    
	    System.out.println("Cluster Centers: ");
	    	    
	    int lv_i = 0;
	    
	    for (Vector center: centers) {
	    	
	      System.out.println(center);
	      
	      lv_centroid[lv_i] = center.toString()
	    		  					.replace("[", "")
	    		  					.replace("]", "");
	      
	     // System.out.println("CENTER:" + lv_centroid[lv_i]);
	      
	      lv_i ++;
	      
	    }	        
	    
	    Dataset<Row> lt_res = model.transform(lv_vector).sort(col("prediction").desc());	    	    
	    
	    lt_res = lt_res.withColumn("CENTROID", functions.lit(lv_centroid))
	    			   .drop(lc_feat);
	    
	    //cl_util.m_save_csv(lt_res.drop(col("features")), "DDoS-Kmeansdrop");
	    
	    //cl_util.m_save_json(lt_res, "DDoS-Kmeansdrop");
	    
	    cl_util.m_show_dataset(lt_res, "Kmeans DDdos");
	    
	    //cl_util.m_save_log(lt_res, gc_kmeans_ddos);    
	    	    
	    cl_util.m_time_end();
	    	    	    
	}
	
	public void m_start_kmeans_ScanPort(SparkSession lv_session, Dataset<Row> lt_data, String lv_service) {
		
		Dataset<Row> lt_res;
		
		gv_tipo = lv_service; //Fazer filtro por PROTOCOLO TAMBÈM?
		
		lt_res = m_normaliza_analise_ScanPort(lt_data);
		
//		/m_ddos_kmeans(lt_res, lv_session);
		
	}
	
	public Dataset<Row> m_normaliza_analise_ScanPort(Dataset<Row> lt_data){
		
		Dataset<Row> lt_res;
		
		cl_util.m_time_start();				
		
		//Query que agrupa todas as portas de Origem Por IP e Minuto
		
		lt_res = lt_data.select( gc_orig_h  ,
								 gc_orig_p  ,
								 gc_resp_h  ,
				                 gc_resp_p  ,
				                 gc_proto   ,
				                 gc_service ,
				                 gc_ts,
				                 gc_ts_code,
				                 gc_duration,
				                 gc_o_bytes,
				                 gc_r_bytes,
				                 gc_orig_pkts, 
				                 gc_orig_bytes,
				                 gc_resp_pkts,	
				                 gc_resp_bytes )					   
					   //.filter(col(gc_service).equalTo(gv_tipo))
					   .filter(col(gc_proto).equalTo("tcp"))    					   
					   .withColumn(gc_ts, date_format(col(gc_ts), gc_format))				   					  
					   .groupBy( col(gc_orig_h),
							     col(gc_orig_p),
							     col(gc_resp_h), 							      
							     col(gc_proto),  
		                         //col(gc_service), //Filtro por protocolo só agrupa por protocolo
		                         col(gc_ts))
					    .agg(sum(gc_duration),
							sum(gc_orig_pkts), 
							sum(gc_o_bytes),
							sum(gc_orig_bytes),
							sum(gc_resp_pkts),	
							sum(gc_r_bytes),
							sum(gc_resp_bytes),
							count("*"))
					   .withColumnRenamed("count(1)", gc_count)
					   .withColumnRenamed(lc_duration, gc_duration)
					   .withColumnRenamed(lc_orig_pkts, gc_orig_pkts)
					   .withColumnRenamed(lc_o_bytes, gc_o_bytes)
					   .withColumnRenamed(lc_orig_bytes, gc_orig_bytes) //Podem ser imprecisos de acordo com o BRO
					   .withColumnRenamed(lc_resp_pkts, gc_resp_pkts)
					   .withColumnRenamed(lc_r_bytes, gc_r_bytes)
					   .withColumnRenamed(lc_resp_bytes, gc_resp_bytes); //Podem ser imprecisos de acordo com o BRO				   
					   		
		lt_res = lt_res.sort(gc_orig_h,
							 gc_ts,
							 gc_count);
		
		lt_res = lt_res.withColumn(gc_ts_filtro, functions.lit(gv_stamp_filtro))						   
			           .withColumn(gc_ts_code, functions.lit(gv_stamp))
			           .withColumn(gc_rowid, functions.monotonically_increasing_id())
			           .withColumn(gc_ts, to_timestamp(col(gc_ts),gc_format)); //para salvar no banco coloca em timestamp novamente
		
		cl_util.m_show_dataset(lt_res, "1) Normaliza Kmeans");									
		
		//m_IpOrig_ForTime(lt_res);
		
		cl_util.m_save_csv(lt_res, "ORIG_H_P_RESP3");
		
		cl_util.m_time_end();
		
		return lt_res;
		
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
