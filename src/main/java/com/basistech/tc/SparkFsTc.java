/*
* Copyright 2016 Basis Technology Corp.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


package com.basistech.tc;

import com.google.common.jimfs.Jimfs;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;

public final class SparkFsTc {
    private SparkFsTc() {
        //
    }
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("File System Test Case");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> logData = sc.parallelize(Collections.singletonList("foo"));
        System.out.println(logData.getNumPartitions());
        logData.mapPartitions(itr -> {
            FileSystem fs = Jimfs.newFileSystem();
            Path path = fs.getPath("/root");
            URI uri = path.toUri();
            Paths.get(uri); // expect this to go splat.
            return null;
        }).collect();
    }
}
