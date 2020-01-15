package com.rustleund.dcchallenge.util;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Utilities related to {@link java.util.function} lambdas.
 */
public final class LambdaUtil {
	private LambdaUtil() {
		//
	}

	/**
	 * Call a {@link CheckedFunction}. If the checked {@link Exception} from the checked function is thrown, propagate it as an unchecked exception. The actual instance thrown by the
	 * {@link CheckedFunction} will be thrown by this call in this case, it is just not declared. Be sure to handle this checked exception appropriately, even though it is not declared to be thrown.
	 * 
	 * @param checkedFunction The {@link CheckedFunction} to call
	 * @return A {@link Function} that calls the given {@link CheckedFunction} as described above
	 */
	public static <T, R> Function<T, R> applyPropagate(CheckedFunction<T, R, Exception> checkedFunction) {
		return t -> {
			try {
				return checkedFunction.apply(t);
			} catch (Exception e) {
				throwAsUnchecked(e);
				return null;
			}
		};
	}

	/**
	 * Call a {@link CheckedConsumer}. If the checked {@link Exception} from the {@link CheckedConsumer} is thrown, propagate it as an unchecked exception. The actual instance thrown by the
	 * {@link CheckedConsumer} will be thrown by this call in this case, it is just not declared. Be sure to handle this checked exception appropriately, even though it is not declared to be thrown.
	 * 
	 * @param checked The {@link CheckedConsumer} to call when the resultant {@link Consumer}'s {@link Consumer#accept(Object)} is called
	 * @return A {@link Consumer} that calls the given {@link CheckedConsumer} as described above
	 */
	public static <T> Consumer<T> acceptPropagate(CheckedConsumer<T, Exception> checked) {
		return t -> {
			try {
				checked.accept(t);
			} catch (Exception e) {
				throwAsUnchecked(e);
			}
		};
	}

	/**
	 * Turn a {@link CheckedConsumer} into a {@link Consumer}, where another {@link Consumer} is called if the {@link CheckedConsumer} throws an exception. Either way, the returned {@link Consumer}
	 * will return noramlly.
	 * 
	 * @param checkedConsumer The {@link CheckedConsumer} used to process each entry
	 * @param exceptionHandler A {@link Consumer} to call with any {@link Exception} thrown by the {@link CheckedConsumer}, including any runtime exceptions
	 * @return A {@link Consumer} that will call the {@link CheckedConsumer} with the same parameter, handling any {@link Exception}s (checked and runtime) by passing them to the given exception
	 *         handler {@link Consumer} and returning normally
	 */
	public static <T> Consumer<T> acceptWithExceptionHandler(CheckedConsumer<T, Exception> checkedConsumer, Consumer<Exception> exceptionHandler) {
		return t -> {
			try {
				checkedConsumer.accept(t);
			} catch (Exception e) {
				exceptionHandler.accept(e);
			}
		};
	}

	@SuppressWarnings("unchecked")
	private static <E extends Throwable> void throwAsUnchecked(Exception exception) throws E {
		throw (E) exception;
	}

	/**
	 * Interface just like {@link Function} that allows the throwing of checked {@link Exception}s.
	 *
	 * @see Function
	 */
	@FunctionalInterface
	public interface CheckedFunction<T, R, X extends Exception> {
		/**
		 * @throws X
		 * @see Function#apply(Object)
		 */
		R apply(T t) throws X;
	}

	/**
	 * Interface just like {@link Consumer} that allows the throwing of checked {@link Exception}s.
	 *
	 * @see Consumer
	 */
	@FunctionalInterface
	public interface CheckedConsumer<T, X extends Exception> {
		/**
		 * @throws X
		 * @see Consumer#accept(Object)
		 */
		void accept(T t) throws X;
	}
}
