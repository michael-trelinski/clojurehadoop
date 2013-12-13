(ns clojurehadoop.core
  (:use [clojure.pprint :only (pprint)])
  (:require [clojure.reflect :as r])
  (:import
    [clojurehadoop.javaimpl ClojureHadoopMapReduce ClojureHadoopMapper ClojureHadoopCombiner ClojureHadoopReducer]
    [org.apache.hadoop.mapreduce Job]
    [org.apache.hadoop.mapreduce.lib.output TextOutputFormat]
    [org.apache.hadoop.mapreduce.lib.input TextInputFormat]
    [org.apache.hadoop.fs Path FileSystem]
    [org.apache.hadoop.conf Configuration]))

(defn run-job
  "runs a job (blocking) and returns true if successful"
  [^Job job]
  (.waitForCompletion job true))

(defn mapper-output-types
  [^Job job output-key output-value]
  (.setMapOutputKeyClass job output-key)
	(.setMapOutputValueClass job output-value)
 job)

(defn output-types
  [^Job job output-key output-value]  
	(.setOutputKeyClass job output-key)
  (.setOutputValueClass job output-value)
  job)

(defn make-job
  [name]
  (doto 
    (new Job)
    (.setJarByClass ClojureHadoopMapReduce)
    (.setJobName name)))

(defn textinput
  [^Job job ^Path path]
  (.setInputFormatClass job TextInputFormat)
  (TextInputFormat/addInputPath job path)
  job)

(defn textoutput
  [^Job job ^Path path]
  (TextOutputFormat/setOutputPath job path)
  (.setOutputFormatClass job TextOutputFormat)
  job)

(defn fresh-textoutput
  [^Job job ^Path path]
  (.delete (FileSystem/get (.getConfiguration job)) path true)
  (textoutput job path))

(defn mapper-output-types
  [^Job job output-key-type output-value-type]
  (doto job
    (.setMapOutputKeyClass output-key-type)
    (.setMapOutputValueClass output-value-type)))

(defn reducer-output-types
  [^Job job output-key-type output-value-type]
  (doto job
    (.setOutputKeyClass output-key-type)
    (.setOutputValueClass output-value-type)))

(defn number-of-reducers
  [^Job job number]
  (doto job (.setNumReduceTasks number)))

(defn mapper
  ([^Job job mapfn-ns mapfn-name]
    (let [fn-meta (meta (ns-resolve mapfn-ns mapfn-name))
          fn-ns (str (:ns fn-meta))
          fn-name (str (:name fn-meta))]
    (.setMapperClass job ClojureHadoopMapper)
    (ClojureHadoopMapReduce/setMapperFunction job fn-ns fn-name)		
    job))
  ([^Job job mapfn]
    (.setMapperClass job ClojureHadoopMapper)
    (ClojureHadoopMapReduce/setMapperFunction job (str mapfn))		
    job))

(defn combiner
  ([^Job job reducerfn-ns reducerfn-name]
    (let [fn-meta (meta (ns-resolve reducerfn-ns reducerfn-name))
          fn-ns (str (:ns fn-meta))
          fn-name (str (:name fn-meta))]
    (.setCombinerClass job ClojureHadoopCombiner)
    (ClojureHadoopMapReduce/setCombinerFunction job fn-ns fn-name)		
    job))
  ([^Job job reducerfn]
    (.setCombinerClass job ClojureHadoopCombiner)
    (ClojureHadoopMapReduce/setCombinerFunction job (str reducerfn))
    job))

(defn reducer
  ([^Job job reducerfn-ns reducerfn-name]
    (let [fn-meta (meta (ns-resolve reducerfn-ns reducerfn-name))
          fn-ns (str (:ns fn-meta))
          fn-name (str (:name fn-meta))]
      (.setReducerClass job ClojureHadoopReducer)
      (ClojureHadoopMapReduce/setReducerFunction job fn-ns fn-name)		
      job))
  ([^Job job reducerfn]
    (.setReducerClass job ClojureHadoopReducer)
    (ClojureHadoopMapReduce/setReducerFunction job (str reducerfn))))

(defn path
  [s]
  (Path. s))

(defn per-files
  [filepath filefn]
  (doseq [_ (.listFiles (java.io.File. filepath))]
    (println "processing: " _)
    (filefn (slurp _))))

(defn print-job
  [^Job job]
  (println "Job: " (.getJobName job))
  (println "Configuration: " (.getConfiguration job))
  ;(println "Grouping Comparator: " (.getGroupingComparator job))
  ;(println "Partitioner: " (.getPartitionerClass job))
  ;(println "Sort Comparator: " (.getSortComparator job))
  (println "Mapper: " (.getMapperClass job))
  (println "Map Output Key: " (.getMapOutputKeyClass job))
  (println "Map Output Value: " (.getMapOutputValueClass job))
  (println "Number of Reduce Tasks: " (.getNumReduceTasks job))
  (println "Output Format Class: " (.getOutputFormatClass job))
  (println "Output Key Class: " (.getOutputKeyClass job))
  (println "Output Value Class: " (.getOutputValueClass job))
  (println "Reducer Class: " (.getReducerClass job))
  (println "Combiner Class: " (.getCombinerClass job)))
  
