from threading import Thread
from collections import deque
from queue import Queue
from collections import Mapping, Sequence
from functools import wraps


__all__ = ["Pipeline"]


class Pipeline(object):
    """Pipeline of functions to apply to elements of an iterable

    Sequentially apply one or more functions to input elements.
    Each applied function accepts the output from the previous stage as input
    parameter(s) and passes its output forward in the pipeline (sequence)
    as input to the next stage. If a function returns 'None' that item
    stops moving forward in the pipeline. This feature can help functions
    be used as filters.

    Each element in the `iterable` provided at init is passed as input
    to the first 'applied' function and is what is used to start the pipeline.

    The pipeline of functions can be run in a multi-threaded environment.
    The number of threads to use during processing is provided at initilization
    and acts as a thread pool for all functions in the pipeline. For example,
    if Pipeline(some_iterable, threads=50) is initialized and 3 functions
    are applied then those 3 functions will be executed by sharing the 50 threads.
    Each function does not have 50 threads to itself.

    Parameters
        iterable: Iterable
            each element is an input to the first stage of the pipeline
        threads: int
            number of threads to use when running the pipeline.
            must be greater than 0. defaults to 1

    Examples
        Simple, unrealistic example to demo how to use the Pipeline:
            >>> input_numbers = [10,20,30]
            >>> mult_by_10 = lambda x: x*10
            >>> div_by_3 = lambda x: x/3
            >>> output, errors = Pipeline(input_numbers).apply(mult_by_10)
                                                        .apply(div_by_3)
                                                        .run()
            >>> output
            [33.333333333333336, 66.66666666666667, 100.0]
            >>> errors
            []
        More realistic example:
            fetching ticker prices, parsing response, updating database:
                >>> tickers = ["XOM", "BP", "CVX", "KMI"]
                >>> pl = Pipeline(tickers, threads=10)
                >>> out, err = pl.apply(http_request_price)
                                .apply(parse_response)
                                .star_apply(update_db)
                                .run()
    """
    def __init__(self, iterable, threads=1):
        if not hasattr(iterable, "__iter__"):
            raise ValueError("provided `iterable` is not iterable")
        if threads < 1:
            raise ValueError("`threads` param must be greater than 0")
        self._iterable = iterable
        self._funcs = self._init_funcs()
        self._num_threads = threads

    def _init_funcs(self):
        return []

    def _save_func_arg(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            self, *args = args
            if len(args)>1:
                raise TypeError("received {} arguments, expected 1".format(len(args)))
            applied_func = func(self, *args, **kwargs)
            if not callable(applied_func.func):
                raise ValueError("provided `func` is not callable")
            self._funcs.append(applied_func)
            return self
        return wrapper

    @_save_func_arg
    def apply(self, func):
        """
        Input to this stage in the pipeline is provided as the parameter
        to the provided `func`

        Parameters:
            func: callable

        Raises:
            ValueError: provided `func` is not callable

        Returns:
            self
        """
        return ApplyCallable(func)

    @_save_func_arg
    def star_apply(self, func):
        """
        Unpacks the input to this stage in the pipeline to the params
        of the provided `func`. An exception is raised when running the pipeline
        if the input is not of type 'Sequence' or 'Mapping'. Sequences
        are applied as positional args to the provided `func`. Mappings
        are applied as keyword args to the provided `func`

        Parameters:
            func: callable

        Raises:
            ValueError: provided `func` is not callable

        Returns:
            self
        """
        return StarApplyCallable(func)

    @_save_func_arg
    def iter_apply(self, func):
        """
        Flattens the input to this stage of the pipeline by appling
        the provided `func` to each element of the input iterable
        and moving the result of `func` forward in the pipeline. An
        exception is raised when running the pipeline if the input
        is not iterable

        Parameters:
            func: callable

        Raises:
            ValueError: provided `func` is not callable

        Returns:
            self
        """
        return IterApplyCallable(func)

    def run(self):
        """
        Run the elements from the initialized `iterable` through the
        sequence of 'applied' (apply, star_apply, iter_apply) functions.
        Blocks until all elements have been processed

        Raises:
            AttributeError: no functions have been applied to the Pipeline instance

        Returns:
            tuple: position 1 list of output, position 2 list of exception objects
        """
        if not self._funcs:
            raise AttributeError("at least one function must "
                                 "be applied before running pipeline")

        def start_pipeline():
            for _ in range(self._num_threads):
                WorkerThread(input_queue, output_queue).start()
            for item in self._iterable:
                item_obj = Item(item, self._funcs.copy())
                input_queue.put(item_obj)

        def get_result():
            output_queue.close()
            output, errors = [], []
            for item in output_queue:
                if item.error:
                    errors.append(item.error)
                else:
                    output.append(item.value)
            return output, errors

        input_queue = Queue()
        output_queue = IterableQueue()
        start_pipeline()
        input_queue.join()
        self._funcs = self._init_funcs()
        return get_result()


class ApplyCallable(object):
    def __init__(self, func):
        self.func = func

    def __call__(self, value):
        return self.func(value)


class StarApplyCallable(ApplyCallable):

    def __call__(self, value):
        if isinstance(value, Sequence):
            return self.func(*value)
        elif isinstance(value, Mapping):
            return self.func(**value)
        else:
            raise TypeError("`func` provided to star_apply "
                            "was called with incorrect type. "
                            "expected Mapping or Sequence not "
                            "{arg_type}".format(arg_type=type(value)))


class IterApplyCallable(ApplyCallable):
    pass


class Item(object):
    def __init__(self, value, funcs):
        self.init_value = value
        self.value = value
        self.funcs = deque(funcs)
        self.error = None

    def get_next_func(self):
        return self.funcs.popleft()


class WorkerThread(Thread):
    def __init__(self, input_queue, output_queue):
        self._input_queue = input_queue
        self._output_queue = output_queue
        super().__init__(daemon=True)

    def _proces_iterapply(self, item, func):
        try:
            for val in item.value:
                new_item_funcs = [ApplyCallable(func.func), *item.funcs.copy()]
                new_item = Item(val, new_item_funcs)
                self._input_queue.put(new_item)
        except TypeError as err:
            item.error = TypeError("`func` provided to iter_apply "
                                   "was called with incorrect type. "
                                   "expected Iterable not "
                                   "{arg_type}".format(arg_type=type(item.value)))
            self._output_queue.put(item)

    def _process_apply(self, item, func):
        try:
            item.value = func(item.value)
        except Exception as err:
            #func raised an exception. move the item to output queue
            item.error = err
            self._output_queue.put(item)
        else:
            #move the item back into the queue if func returned a value
            if item.value is not None:
                self._input_queue.put(item)

    def run(self):
        while True:
            item = self._input_queue.get()
            try:
                func = item.get_next_func()
            except IndexError:
                #pipeline is done. move item to output queue
                self._output_queue.put(item)
            else:
                if isinstance(func, IterApplyCallable):
                    self._proces_iterapply(item, func)
                else:
                    self._process_apply(item, func)
            finally:
                self._input_queue.task_done()


class IterableQueue(Queue):
    _close_item = object()
    
    def __iter__(self):
        while True:
            try:
                item = self.get()
                if item is self._close_item:
                    return
                else:
                    yield item
            finally:
                self.task_done()

    def close(self):
        self.put(self._close_item)
