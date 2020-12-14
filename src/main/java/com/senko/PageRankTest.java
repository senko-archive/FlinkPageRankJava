package com.senko;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.linkanalysis.PageRank;
import org.apache.flink.types.NullValue;

import java.util.Arrays;
import java.util.List;

public class PageRankTest {



    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        String inputFile = null;
        String outputFile = null;
        Integer iter = 5;

        if(params.has("input")) {
            inputFile = params.get("input");
        }
        else {
            System.out.println("please enter an input file");
        }

        if(params.has("output")) {
            outputFile = params.get("output");
        }
        else {
            System.out.println("please enter an output file");
        }

        if(params.has("iter")) {
            iter = params.getInt("iter");
        }



        // ilk once file dan oku
        Graph<String, NullValue, NullValue> graph2 = Graph.fromCsvReader(inputFile, env).fieldDelimiterEdges("\t").keyType(String.class);
        DataSet<String> vertexIDs = graph2.getVertexIds();
        vertexIDs.print();

        //List<PageRank.Result<String>> result2 = graph2.run(new PageRank<String, NullValue, NullValue>(0.85, iter)).collect();
        //System.out.println(result2);

        graph2.run(new PageRank<String, NullValue, NullValue>(0.85, iter)).writeAsText(outputFile);
        env.execute();

        // daha sonra hdfs ten okumaya calis




    }

}
