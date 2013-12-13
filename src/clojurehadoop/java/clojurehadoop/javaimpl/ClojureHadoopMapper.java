package clojurehadoop.javaimpl;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import clojure.lang.IFn;
import clojure.lang.RT;

public class ClojureHadoopMapper<IK, IV, OK, OV> extends Mapper<IK, IV, OK, OV> {

	protected static IFn setupFn = null;
	protected static IFn mapperFn = null;
	protected static IFn cleanupFn = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		RT.init();

		if (ClojureHadoopMapReduce.hasMapperSetupFunction(context
				.getConfiguration())) {
			setupFn = (IFn) RT.var(
					ClojureHadoopMapReduce
							.getMapperSetupFunctionNamespace(context
									.getConfiguration()),
					ClojureHadoopMapReduce.getMapperSetupFunctionName(context
							.getConfiguration()));
			setupFn.invoke(context);
		} else if (ClojureHadoopMapReduce.getMapperSetupFunctionString(context.getConfiguration()) != null) {
			setupFn = (IFn) RT.var("clojure.core", "eval").invoke(
					RT.var("clojure.core", "read-string").invoke(
							ClojureHadoopMapReduce
									.getMapperSetupFunctionString(context
											.getConfiguration())));
		}

		if (ClojureHadoopMapReduce.hasMapperCleanupFunction(context
				.getConfiguration())) {
			cleanupFn = (IFn) RT.var(ClojureHadoopMapReduce
					.getMapperCleanupFunctionNamespace(context
							.getConfiguration()), ClojureHadoopMapReduce
					.getMapperCleanupFunctionName(context.getConfiguration()));
		} else if (ClojureHadoopMapReduce.getMapperCleanupFunctionString(context.getConfiguration()) != null) {
			cleanupFn = (IFn) RT.var("clojure.core", "eval").invoke(
					RT.var("clojure.core", "read-string").invoke(
							ClojureHadoopMapReduce
									.getMapperCleanupFunctionString(context
											.getConfiguration())));
		}

		if (ClojureHadoopMapReduce.getMapperFunctionNamespace(context
				.getConfiguration()) != null
				&& ClojureHadoopMapReduce.getMapperFunctionName(context
						.getConfiguration()) != null)
			mapperFn = (IFn) RT.var(ClojureHadoopMapReduce
					.getMapperFunctionNamespace(context.getConfiguration()),
					ClojureHadoopMapReduce.getMapperFunctionName(context
							.getConfiguration()));
		else {
			mapperFn = (IFn) RT.var("clojure.core", "eval").invoke(
					RT.var("clojure.core", "read-string").invoke(
							ClojureHadoopMapReduce
									.getMapperFunctionString(context
											.getConfiguration())));
		}
	}

	protected void map(IK key, IV value, Context context) throws IOException,
			InterruptedException {
		mapperFn.invoke(key, value, context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		if (cleanupFn != null)
			cleanupFn.invoke(context);
	}

}
