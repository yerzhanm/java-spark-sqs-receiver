package yerzhanm;

import com.amazonaws.regions.Regions;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Main {

    final static Map<String, String> maps = new HashMap<>();

    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws UnsupportedEncodingException, InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("LocalSample");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jssc.receiverStream(
                new SQSReceiver("sample")
                    .at(Regions.AP_SOUTHEAST_2)
        );

        lines.foreachRDD(rdd->{
            rdd.foreach(w->{
                LOGGER.info(w);
            });
        });

        jssc.start();
        jssc.awaitTermination();

    }
}