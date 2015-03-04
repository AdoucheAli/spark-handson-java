package fr.devoxx;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import fr.devoxx.entity.Performer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Exercise1 {

    public static <R> void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("exercise-1")
                .set("spark.cassandra.connection.host", "192.168.0.10");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<CassandraRow> rows = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("music", "performers");

        JavaRDD<Performer> performers = rows.map(row -> new Performer(row.getString("name"), row.getString("style")));

        CassandraJavaUtil.javaFunctions(performers)
                .writerBuilder("music", "performers_by_style", CassandraJavaUtil.mapToRow(Performer.class))
                .saveToCassandra();
    }

}
