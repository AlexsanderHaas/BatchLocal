package start;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class cl_kmeans {


	public void m_kmeans(Dataset<Row> lt_data, SparkSession lv_session) {
		
		Dataset<Row> lt_sum;
		
		lt_sum = lt_data.filter(col("SUM(DURATION)").isNotNull())
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
