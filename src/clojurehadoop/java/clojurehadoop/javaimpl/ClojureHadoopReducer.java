package clojurehadoop.javaimpl;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import clojure.lang.IFn;
import clojure.lang.RT;

public class ClojureHadoopReducer<IK, IV, OK, OV> extends
		Reducer<IK, IV, OK, OV> {

	protected static IFn setupFn = null;
	protected static IFn cleanupFn = null;
	protected static IFn reducerFn = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		RT.init();
		setupFn = null;

		if (ClojureHadoopMapReduce.hasReducerSetupFunction(context
				.getConfiguration())) {
			setupFn = (IFn) RT.var(ClojureHadoopMapReduce
					.getReducerSetupFunctionNamespace(context
							.getConfiguration()), ClojureHadoopMapReduce
					.getReducerSetupFunctionName(context.getConfiguration()));
		} else if (ClojureHadoopMapReduce.getReducerSetupFunctionString(context
				.getConfiguration()) != null) {
			setupFn = (IFn) RT.var("clojure.core", "eval").invoke(
					RT.var("clojure.core", "read-string").invoke(
							ClojureHadoopMapReduce
									.getReducerSetupFunctionString(context
											.getConfiguration())));
		}
		
		if (setupFn != null) 			
			setupFn.invoke(context);

		if (ClojureHadoopMapReduce.hasReducerCleanupFunction(context
				.getConfiguration())) {
			cleanupFn = (IFn) RT.var(ClojureHadoopMapReduce
					.getReducerCleanupFunctionNamespace(context
							.getConfiguration()), ClojureHadoopMapReduce
					.getReducerCleanupFunctionName(context.getConfiguration()));
		} else if (ClojureHadoopMapReduce
				.getReducerCleanupFunctionString(context.getConfiguration()) != null) {
			cleanupFn = (IFn) RT.var("clojure.core", "eval").invoke(
					RT.var("clojure.core", "read-string").invoke(
							ClojureHadoopMapReduce
									.getReducerCleanupFunctionString(context
											.getConfiguration())));
		}

		if (ClojureHadoopMapReduce.getReducerFunctionNamespace(context
				.getConfiguration()) != null
				&& ClojureHadoopMapReduce.getReducerFunctionName(context
						.getConfiguration()) != null) {
			reducerFn = (IFn) RT.var(ClojureHadoopMapReduce
					.getReducerFunctionNamespace(context.getConfiguration()),
					ClojureHadoopMapReduce.getReducerFunctionName(context
							.getConfiguration()));
		} else if (ClojureHadoopMapReduce.getReducerFunctionString(context
				.getConfiguration()) != null) {
			reducerFn = (IFn) RT.var("clojure.core", "eval").invoke(
					RT.var("clojure.core", "read-string").invoke(
							ClojureHadoopMapReduce
									.getReducerFunctionString(context
											.getConfiguration())));
		}
	}

	@Override
	protected void reduce(IK key, Iterable<IV> values, Context context)
			throws IOException, InterruptedException {
		reducerFn.invoke(key, values, context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		if (cleanupFn != null)
			cleanupFn.invoke(context);
	}

}
