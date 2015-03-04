package fr.devoxx;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import fr.devoxx.entity.PerformerDistributionByStyle;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Exercise2 {

    public static <R> void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("exercise-2")
                .set("spark.cassandra.connection.host", "192.168.0.10");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<CassandraRow> rows = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("music", "performers");

        JavaRDD<PerformerDistributionByStyle> performersDistributionByStyle = rows
                .map(row -> new Tuple2<>(row.getString("type"), row.getString("style")))
                .mapToPair(t -> new Tuple2<>(t, 1))
                .reduceByKey((x, y) -> x + y)
                .map(row -> new PerformerDistributionByStyle(row._1._1, row._1._2, row._2));

        CassandraJavaUtil.javaFunctions(performersDistributionByStyle)
                .writerBuilder("music", "performers_distribution_by_style",
                        CassandraJavaUtil.mapToRow(PerformerDistributionByStyle.class))
                .saveToCassandra();
    }

}
