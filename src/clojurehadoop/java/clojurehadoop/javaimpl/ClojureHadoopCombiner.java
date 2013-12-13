package clojurehadoop.javaimpl;

import java.io.IOException;

import clojure.lang.RT;
import clojure.lang.IFn;

public class ClojureHadoopCombiner<IK, IV, OK, OV> extends
		ClojureHadoopReducer<IK, IV, OK, OV> {

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		RT.init();
		cleanupFn = null;

		if (ClojureHadoopMapReduce.hasCombinerSetupFunction(context
				.getConfiguration())) {
			setupFn = (IFn) RT.var(ClojureHadoopMapReduce
					.getCombinerSetupFunctionNamespace(context
							.getConfiguration()), ClojureHadoopMapReduce
					.getCombinerSetupFunctionName(context.getConfiguration()));
		} else if (ClojureHadoopMapReduce.getReducerSetupFunctionString(context
				.getConfiguration()) != null) {
			setupFn = (IFn) RT.var("clojure.core", "eval").invoke(
					RT.var("clojure.core", "read-string").invoke(
							ClojureHadoopMapReduce
									.getCombinerSetupFunctionString(context
											.getConfiguration())));
		}

		if (setupFn != null)
			setupFn.invoke(context);

		if (ClojureHadoopMapReduce.hasCombinerCleanupFunction(context
				.getConfiguration())) {
			cleanupFn = (IFn) RT
					.var(ClojureHadoopMapReduce
							.getCombinerCleanupFunctionNamespace(context
									.getConfiguration()),
							ClojureHadoopMapReduce
									.getCombinerCleanupFunctionName(context
											.getConfiguration()));
		} else if (ClojureHadoopMapReduce
				.getReducerCleanupFunctionString(context.getConfiguration()) != null) {
			cleanupFn = (IFn) RT.var("clojure.core", "eval").invoke(
					RT.var("clojure.core", "read-string").invoke(
							ClojureHadoopMapReduce
									.getCombinerCleanupFunctionString(context
											.getConfiguration())));
		}

		if (ClojureHadoopMapReduce.getReducerFunctionNamespace(context
				.getConfiguration()) != null
				&& ClojureHadoopMapReduce.getReducerFunctionName(context
						.getConfiguration()) != null) {
			reducerFn = (IFn) RT.var(ClojureHadoopMapReduce
					.getCombinerReducerFunctionNamespace(context.getConfiguration()),
					ClojureHadoopMapReduce.getCombinerReducerFunctionName(context
							.getConfiguration()));
		} else if (ClojureHadoopMapReduce.getReducerFunctionString(context
				.getConfiguration()) != null) {
			reducerFn = (IFn) RT.var("clojure.core", "eval").invoke(
					RT.var("clojure.core", "read-string").invoke(
							ClojureHadoopMapReduce
									.getCombinerReducerFunctionString(context
											.getConfiguration())));
		}
	}

}
