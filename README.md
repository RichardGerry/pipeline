# Pipeline

Pipeline allows you to run a sequence of functions in order
where the output of one function becomes the input the next

Written in python - it is easy to use, multi-threaded capable, helps logically break work into
concise functions, removes messy looping and callbacks that would otherwise 
be needed to chain together function inputs and outputs


## Usage

As an example - lets say you wanted to fetch the latest stock price for a ticker
and then update a database with the result. You would define a sequence of steps
that would perform that action.

If you had a large batch of tickers you would want to perform the same sequence of steps
to each item in the batch. This is where Pipeline can help (especially if your steps
require a lot of IO - you can use multiple threads to perform your steps)

Those items (an iterable) are provided when intializing an instance of Pipeline. With
a Pipeline instance you define that sequence of steps through 'apply' methods.
Those steps are then started by calling the 'run' method.


To begin - import Pipeline class

```pycon
>>> from pipeline import Pipeline
```

Pipeline is initialize with:
* an iterable - each element is input to the first applied function
* optionally a number of threads - size of the thread pool which is shared across all 'applied' functions

Starting the stock ticker example described above a pipeline is initialized with some tickers. This pipeline will be run with 10 threads to perform all the work

```pycon
>>> tickers = ["XOM", "BP", "CVX", "KMI"]
>>> pipe = Pipeline(tickers, threads=10)
```

With a pipeline instance created functions are then applied to it by calling 'apply' methods (apply, star_apply, iter_apply)
These methods each accept one parameter: a callable. 

That callable is called with the output from the previously 'applied' function as its input parameter(s). 
The output of the callable is then the input to the next 'applied' function.

Here are a few examples of functions that might perform the steps for this example

```pycon
>>> def ticker_http_request(ticker):
		return requests.get("some_url/"+ticker)
>>> def parse_response(response):
		do some parsing ....
		return {"ticker":some_ticker, "price":some_price}
>>> def ticker_update(ticker, price):
		do some updating ....
		return ticker
```

Those functions are then 'applied' to the pipeline

Note:
*star_apply unpacks input to its parameters either positionally or kwargs based on the input type of Sequence or Mapping. parse_response returns a dict (Mapping) so the dict is unpacked to the kwargs of ticker_update

```pycon
>>> pipe.apply(ticker_http_request)
		.apply(parse_response)
		.star_apply(ticker_update)
```

The pipeline is then started by calling its 'run' method. 

Each item in the iterable provided at init will be sent through the pipeline individually with the output from one step serving as the input to the next step.

```pycon
>>> output, errors = pipe.run()
```

'run' returns a tuple - first position is the output (a list containing the return values
from the last applied function), second position is the errors (a list of any exceptions raised at any point
in the pipeline)


A few other notes:
* if a function returns 'None' then the item stops moving forward in the pipeline. This can be used as a way to filter results
* iter_apply is another method (not used above) and can be used to flatten results. If a function returns an iterable but each item in the iterable needs to be further processed individually you would call iter_apply. Each item in the input iterable would have the 'func' (iter_apply param) applied to it along with any subsequently applied functions
* 'run' method returns exception objects (tuple second position) which can be used with the traceback module if you need a full stack trace
* 'run' method blocks until all items have been processed
* star_apply was used in the example above to unpack a Mapping into keyword args. It can also be used to unpack a Sequence into positional args

