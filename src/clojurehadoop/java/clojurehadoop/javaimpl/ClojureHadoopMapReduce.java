package clojurehadoop.javaimpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class ClojureHadoopMapReduce {

	private static final String CLOJURE_HADOOP_MAPPER_CLEANUP_FN_STRING = "clojurehadoop.mapper.cleanup.fn";
	private static final String CLOJURE_HADOOP_MAPPER_SETUP_FN_STRING = "clojurehadoop.mapper.setup.fn";
	private static final String CLOJURE_HADOOP_REDUCER_CLEANUP_FN_STRING = "clojurehadoop.reducer.cleanup.fn";
	private static final String CLOJURE_HADOOP_REDUCER_FN_STRING = "clojurehadoop.reducer.fn";
	private static final String CLOJURE_HADOOP_REDUCER_SETUP_FN_STRING = "clojurehadoop.reducer.setup.fn";
	private static final String CLOJUREHADOOP_MAPPER_FN_STRING = "clojurehadoop.mapper.fn";
	
	private static final String CLOJUREHADOOP_COMBINER_CLEANUP_NAME = "clojurehadoop.combiner.cleanup.name";
	private static final String CLOJUREHADOOP_COMBINER_CLEANUP_NS = "clojurehadoop.combiner.cleanup.ns";
	private static final String CLOJUREHADOOP_COMBINER_NAME = "clojurehadoop.combiner.name";
	private static final String CLOJUREHADOOP_COMBINER_NS = "clojurehadoop.combiner.ns";
	
	private static final String CLOJUREHADOOP_COMBINER_SETUP_NAME = "clojurehadoop.combiner.setup.name";
	private static final String CLOJUREHADOOP_COMBINER_SETUP_NS = "clojurehadoop.combiner.setup.ns";
	private static final String CLOJUREHADOOP_MAPPER_CLEANUP_NAME = "clojurehadoop.mapper.cleanup.name";
	private static final String CLOJUREHADOOP_MAPPER_CLEANUP_NS = "clojurehadoop.mapper.cleanup.ns";
	private static final String CLOJUREHADOOP_MAPPER_NAME = "clojurehadoop.mapper.name";
	private static final String CLOJUREHADOOP_MAPPER_NS = "clojurehadoop.mapper.ns";
	private static final String CLOJUREHADOOP_MAPPER_SETUP_NAME = "clojurehadoop.mapper.setup.name";
	private static final String CLOJUREHADOOP_MAPPER_SETUP_NS = "clojurehadoop.mapper.setup.ns";
	
	private static final String CLOJUREHADOOP_REDUCER_CLEANUP_NAME = "clojurehadoop.reducer.cleanup.name";
	private static final String CLOJUREHADOOP_REDUCER_CLEANUP_NS = "clojurehadoop.reducer.cleanup.ns";
	private static final String CLOJUREHADOOP_REDUCER_NAME = "clojurehadoop.reducer.name";
	private static final String CLOJUREHADOOP_REDUCER_NS = "clojurehadoop.reducer.ns";
	private static final String CLOJUREHADOOP_REDUCER_SETUP_NAME = "clojurehadoop.reducer.setup.name";
	private static final String CLOJUREHADOOP_REDUCER_SETUP_NS = "clojurehadoop.reducer.setup.ns";
	private static final String CLOJUREHADOOP_COMBINER_SETUP_FN_STRING = "clojurehadoop.combiner.setup.fn";
	private static final String CLOJUREHADOOP_COMBINER_CLEANUP_FN_STRING = "clojurehadoop.combiner.cleanup.fn";
	private static final String CLOJUREHADOOP_COMBINER_FN_STRING = "clojurehadoop.combiner.fn";
	

	public static String getCleanupFunctionName(Configuration conf) {
		return conf.get(CLOJUREHADOOP_MAPPER_CLEANUP_NAME);
	}

	public static String getCombinerCleanupFunctionName(Configuration conf) {
		return conf.get(CLOJUREHADOOP_COMBINER_CLEANUP_NAME);
	}

	public static String getCombinerCleanupFunctionNamespace(Configuration conf) {
		return conf.get(CLOJUREHADOOP_COMBINER_CLEANUP_NS);
	}

	public static String getCombinerReducerFunctionName(Configuration conf) {
		return conf.get(CLOJUREHADOOP_COMBINER_NAME);
	}

	public static String getCombinerReducerFunctionNamespace(Configuration conf) {
		return conf.get(CLOJUREHADOOP_COMBINER_NS);
	}

	public static String getCombinerSetupFunctionName(Configuration conf) {
		return conf.get(CLOJUREHADOOP_COMBINER_SETUP_NAME);
	}

	public static String getCombinerSetupFunctionNamespace(Configuration conf) {
		return conf.get(CLOJUREHADOOP_COMBINER_SETUP_NS);
	}

	public static String getMapperCleanupFunctionName(Configuration conf) {
		return conf.get(CLOJUREHADOOP_MAPPER_CLEANUP_NAME);
	}

	public static String getMapperCleanupFunctionNamespace(Configuration conf) {
		return conf.get(CLOJUREHADOOP_MAPPER_CLEANUP_NS);
	}

	public static String getMapperCleanupFunctionString(
			Configuration conf) {
		return conf.get(CLOJURE_HADOOP_MAPPER_CLEANUP_FN_STRING);
	}

	public static String getMapperFunctionName(Configuration conf) {
		return conf.get(CLOJUREHADOOP_MAPPER_NAME);
	}

	public static String getMapperFunctionNamespace(Configuration conf) {
		return conf.get(CLOJUREHADOOP_MAPPER_NS);
	}

	public static String getMapperFunctionString(Configuration conf) {
		return conf.get(CLOJUREHADOOP_MAPPER_FN_STRING);
	}

	public static String getMapperSetupFunctionName(Configuration conf) {
		return conf.get(CLOJUREHADOOP_MAPPER_SETUP_NAME);
	}

	public static String getMapperSetupFunctionNamespace(Configuration conf) {
		return conf.get(CLOJUREHADOOP_MAPPER_SETUP_NS);
	}

	public static String getMapperSetupFunctionString(
			Configuration conf) {
		return conf.get(CLOJURE_HADOOP_MAPPER_SETUP_FN_STRING);
	}

	public static String getReducerCleanupFunctionName(Configuration conf) {
		return conf.get(CLOJUREHADOOP_REDUCER_CLEANUP_NAME);
	}

	public static String getReducerCleanupFunctionNamespace(Configuration conf) {
		return conf.get(CLOJUREHADOOP_REDUCER_CLEANUP_NS);
	}

	public static String getReducerCleanupFunctionString(
			Configuration conf) {
		return conf.get(CLOJURE_HADOOP_REDUCER_CLEANUP_FN_STRING);
	}

	public static String getReducerFunctionName(Configuration conf) {
		return conf.get(CLOJUREHADOOP_REDUCER_NAME);
	}

	public static String getReducerFunctionNamespace(Configuration conf) {
		return conf.get(CLOJUREHADOOP_REDUCER_NS);
	}

	public static String getReducerFunctionString(Configuration conf) {
		return conf.get(CLOJURE_HADOOP_REDUCER_FN_STRING);
	}

	public static String getReducerSetupFunctionName(Configuration conf) {
		return conf.get(CLOJUREHADOOP_REDUCER_SETUP_NAME);
	}

	public static String getReducerSetupFunctionNamespace(Configuration conf) {
		return conf.get(CLOJUREHADOOP_REDUCER_SETUP_NS);
	}

	public static String getReducerSetupFunctionString(
			Configuration conf) {
		return conf.get(CLOJURE_HADOOP_REDUCER_SETUP_FN_STRING);
	}
	

	public static boolean hasCombinerCleanupFunction(Configuration conf) {
		return getCombinerCleanupFunctionNamespace(conf) != null && 
				getCombinerCleanupFunctionName(conf) != null;
	}
	

	public static boolean hasCombinerSetupFunction(Configuration conf) {
		return getCombinerSetupFunctionNamespace(conf) != null && 
				getCombinerSetupFunctionName(conf) != null; 
	}

	public static boolean hasMapperCleanupFunction(Configuration conf) {
		return getMapperCleanupFunctionNamespace(conf) != null
				&& getMapperCleanupFunctionName(conf) != null;
	}

	public static boolean hasMapperSetupFunction(Configuration conf) {
		return getMapperSetupFunctionNamespace(conf) != null
				&& getMapperSetupFunctionName(conf) != null;
	}

	public static boolean hasReducerCleanupFunction(Configuration conf) {
		return getReducerCleanupFunctionNamespace(conf) != null && 
				getReducerCleanupFunctionName(conf) != null;
	}
	
	public static boolean hasReducerSetupFunction(Configuration conf) {
		return getReducerSetupFunctionNamespace(conf) != null &&				
				getReducerSetupFunctionName(conf) != null;
	}

	public static void setCombinerCleanupFunction(Job job, String ns,
			String name) {
		job.getConfiguration().set(CLOJUREHADOOP_COMBINER_CLEANUP_NS, ns);
		job.getConfiguration().set(CLOJUREHADOOP_COMBINER_CLEANUP_NAME, name);
	}

	
	public static void setCombinerFunction(Job job, String fn) {
		job.getConfiguration().set(ClojureHadoopMapReduce.CLOJUREHADOOP_COMBINER_FN_STRING, fn);
	}
	
	public static void setCombinerSetupFunction(Job job, String fn) {
		job.getConfiguration().set(ClojureHadoopMapReduce.CLOJURE_HADOOP_REDUCER_SETUP_FN_STRING, fn);
	}
	
	public static void setCombinerCleanupFunction(Job job, String fn) {
		job.getConfiguration().set(ClojureHadoopMapReduce.CLOJURE_HADOOP_REDUCER_CLEANUP_FN_STRING, fn);
	}	
	
	public static void setCombinerFunction(Job job, String ns, String name) {
		job.getConfiguration().set(CLOJUREHADOOP_COMBINER_NS, ns);
		job.getConfiguration().set(CLOJUREHADOOP_COMBINER_NAME, name);
	}

	public static void setCombinerSetupFunction(Job job, String ns, String name) {
		job.getConfiguration().set(CLOJUREHADOOP_COMBINER_SETUP_NS, ns);
		job.getConfiguration().set(CLOJUREHADOOP_COMBINER_SETUP_NAME, name);
	}

	public static void setMapperCleanupFunction(Job job, String ns, String name) {
		job.getConfiguration().set(CLOJUREHADOOP_MAPPER_CLEANUP_NS, ns);
		job.getConfiguration().set(CLOJUREHADOOP_MAPPER_CLEANUP_NAME, name);
	}

	public static void setMapperFunction(Job job, String fn) {
		job.getConfiguration().set(CLOJUREHADOOP_MAPPER_FN_STRING, fn);
	}
	
	public static void setMapperSetupFunction(Job job, String fn) {
		job.getConfiguration().set(ClojureHadoopMapReduce.CLOJURE_HADOOP_MAPPER_SETUP_FN_STRING, fn);
	}
	
	public static void setMapperCleanupFunction(Job job, String fn) {
		job.getConfiguration().set(ClojureHadoopMapReduce.CLOJURE_HADOOP_MAPPER_CLEANUP_FN_STRING, fn);
	}


	public static void setMapperFunction(Job job, String ns, String name) {
		job.getConfiguration().set(CLOJUREHADOOP_MAPPER_NS, ns);
		job.getConfiguration().set(CLOJUREHADOOP_MAPPER_NAME, name);
	}

	public static void setMapperSetupFunction(Job job, String ns, String name) {
		job.getConfiguration().set(CLOJUREHADOOP_MAPPER_SETUP_NS, ns);
		job.getConfiguration().set(CLOJUREHADOOP_MAPPER_SETUP_NAME, name);
	}

	public static void setReducerCleanupFunction(Job job, String ns, String name) {
		job.getConfiguration().set(CLOJUREHADOOP_REDUCER_CLEANUP_NS, ns);
		job.getConfiguration().set(CLOJUREHADOOP_REDUCER_CLEANUP_NAME, name);
	}
	
	public static void setReducerFunction(Job job, String fn) {
		job.getConfiguration().set(CLOJURE_HADOOP_REDUCER_FN_STRING, fn);
	}
	
	public static void setReducerSetupFunction(Job job, String fn) {
		job.getConfiguration().set(ClojureHadoopMapReduce.CLOJURE_HADOOP_REDUCER_SETUP_FN_STRING, fn);
	}
	
	public static void setReducerCleanupFunction(Job job, String fn) {
		job.getConfiguration().set(ClojureHadoopMapReduce.CLOJURE_HADOOP_REDUCER_CLEANUP_FN_STRING, fn);
	}


	public static void setReducerFunction(Job job, String ns, String name) {
		job.getConfiguration().set(CLOJUREHADOOP_REDUCER_NS, ns);
		job.getConfiguration().set(CLOJUREHADOOP_REDUCER_NAME, name);
	}

	public static void setReducerSetupFunction(Job job, String ns, String name) {
		job.getConfiguration().set(CLOJUREHADOOP_REDUCER_SETUP_NS, ns);
		job.getConfiguration().set(CLOJUREHADOOP_REDUCER_SETUP_NAME, name);
	}

	public static String getCombinerSetupFunctionString(
			Configuration conf) {
		return conf.get(CLOJUREHADOOP_COMBINER_SETUP_FN_STRING); 
	}

	public static String getCombinerCleanupFunctionString(
			Configuration conf) {
		return conf.get(CLOJUREHADOOP_COMBINER_CLEANUP_FN_STRING);
	}

	public static Object getCombinerReducerFunctionString(
			Configuration conf) {
		return conf.get(CLOJUREHADOOP_COMBINER_FN_STRING);
	}

}
