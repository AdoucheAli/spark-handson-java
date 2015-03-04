package fr.devoxx;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import fr.devoxx.entity.PerformerDistributionByStyle;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class Exercise3 {

    public static <R> void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("exercise-2")
                .set("spark.cassandra.connection.host", "192.168.0.10");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<CassandraRow> rows = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("music", "performers_distribution_by_style");

        JavaRDD<PerformerDistributionByStyle> performerDistributionByStyle = rows
                .map(row -> new PerformerDistributionByStyle(row.getString("type"), row.getString("style"),
                        row.getInt("count")))
                .filter(p -> !p.getStyle().equals("Unknown"))
                .sortBy(p -> p.getCount(), false, 1);

        List<PerformerDistributionByStyle> top10Bands = performerDistributionByStyle
                .filter(p -> p.getType().equals("band"))
                .take(10);

        List<PerformerDistributionByStyle> top10Artists = performerDistributionByStyle
                .filter(p -> p.getType().equals("artist"))
                .take(10);

        JavaRDD<PerformerDistributionByStyle> merged = sc.parallelize(top10Artists)
                .union(sc.parallelize(top10Bands));

        CassandraJavaUtil.javaFunctions(merged)
                .writerBuilder("music", "top10_styles",
                        CassandraJavaUtil.mapToRow(PerformerDistributionByStyle.class))
                .saveToCassandra();
    }

}
