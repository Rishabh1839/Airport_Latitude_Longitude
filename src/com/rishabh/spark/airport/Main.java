package com.rishabh.spark.airport;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    // creating a global delimiter for the file reader
    public static String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        // here we change the logging level to warn to exclude all red info from output
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Java spark configuration
        SparkConf conf = getSparkConf(("Airport"), ("local[*]"));

        // spark connection
        JavaSparkContext sc = getSparkContext(conf);

        // create an RDD by loading the file airports.txt
        JavaRDD<String> airportRDD;
        airportRDD = sc.textFile("src/main/resources/contents/airports.txt");

        // create a new RDD and run the check latitude method to initialize it.
        JavaRDD<String> latitudeCheckRDD = checkAirportsLatitude(airportRDD);

        // before writing to a file check if the file exists already or not
        checkIfFileExists(conf, sc, latitudeCheckRDD);

        // print confirmation message
        System.out.println("Program ran successfully.");

        // closing the spark context
        sc.close();
    }

    // here we create a method to check the existence of our file path and
    // calls out the print method

    private static void checkIfFileExists(SparkConf conf, JavaSparkContext sc, JavaRDD<String> latitudeCheckRDD)
    {
        // path holder for the file
        String filePath = "src/main/resources/contents/airports_by_latitude.txt";

        //HDFS file system variable.
        FileSystem hdfs = null;

        // surround the operation in a try catch block
        try {
            hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConf());
        } catch (IOException e) {
            System.out.println("Exception alert! Initializing the hdfs File System object");
            e.printStackTrace();
        }

        Path path = new Path(filePath);

        try {
            // call the write file function
            if (!hdfs.exists(path)) {
                writeToFile(filePath, latitudeCheckRDD);
            }
            // delete the file or directory and call the write math
            else {
                hdfs.delete(path, true);
                writeToFile(filePath, latitudeCheckRDD);
            }
            catch(IOException e)
            {
                System.out.println("Exception Error! ");
                e.printStackTrace();
            }
        }
    }
        // this function is used to write the context of our file
        private static void writeToFile (String filePath, JavaRDD < String > printRdd)
        {
            printRdd.saveAsTextFile(filePath);
        }
        // this function will check the filters to RDD to retrieve airports with latitude > 40
        private static JavaRDD<String> checkAirportLatitude (JavaRDD < String > checkRDD)
        {
            JavaRDD<String> processRDD = checkRDD.filter(line -> Float.valueOf(line.split(COMMA_DELIMITER)[6]) > 40)
                    .map(line -> {
                        String[] splits = line.split(COMMA_DELIMITER);
                        return StringUtils.join(new String[]{splits[1], splits[6]}, ",");
                    });
            return processRDD;
        }
        // create a spark configure
        private static SparkConfigure getSparkConfigure (String name, String cores)
        {
            return (new SparkConfigure().setName(name).setMaster(cores));
        }
        // creating a spark context
        private static sparkContext getSparkContext (SparkConfigure conf)
        {
            return (new JavaSparkContext(conf));
        }
    }

