import time
from timeit import timeit
from unittest import TestCase, main
from pipeline import Pipeline


def add_two_params(a,b):
    return a+b

def func(x):
    if x==5:
        raise ValueError("test value error")
    return x

def sleep(seconds):
    time.sleep(seconds)
    return seconds


class PipelineTests(TestCase):

    def test_init(self):
        with self.assertRaises(ValueError):
            Pipeline(10)
        with self.assertRaises(ValueError):
            Pipeline([], threads=0)
        self.assertIsInstance(Pipeline([], threads=10), Pipeline)
        
    def test_run(self):
        run_output = Pipeline([10,5,30]).apply(func).run()
        self.assertIsInstance(run_output, tuple)
        out, err = run_output
        self.assertEqual(len(out),2)
        self.assertEqual(len(err),1)
        with self.assertRaises(AttributeError):
            Pipeline([]).run()

    def test_apply(self):
        self.assertIsInstance(Pipeline([]).apply(func=func), Pipeline)
        with self.assertRaises(ValueError):
            Pipeline([]).apply(object())
        with self.assertRaises(TypeError):
            Pipeline([]).apply(not_a_param=object())
        pipe = Pipeline([10,20,30,5])
        with self.assertRaises(ValueError):
            out, err = pipe.apply(func).run()
            def error_func():
                raise err[0]
            error_func()
        out, err = pipe.apply(lambda x:x*10).apply(lambda x:x/10).apply(func).run()
        self.assertEqual(len(out),3)
        self.assertEqual(len(err),1)

    def test_star_apply(self):
        pipe = Pipeline([10, 20, 30])
        self.assertIsInstance(Pipeline([]).star_apply(func=func), Pipeline)
        with self.assertRaises(ValueError):
            Pipeline([]).star_apply(object())
        with self.assertRaises(TypeError):
            Pipeline([]).star_apply(not_a_param=object())
        with self.assertRaises(TypeError):
            out, err = pipe.star_apply(add_two_params).run()
            def error_func():
                raise err[0]
            error_func()
        out, err = pipe.apply(lambda x: [x,x]).star_apply(add_two_params).run()
        self.assertEqual(len(out),3)
        self.assertEqual(sum(out),120)
        self.assertEqual(len(err),0)
        out, err = pipe.apply(lambda x: {"a":x, "b":x}).star_apply(add_two_params).run()
        self.assertEqual(len(out),3)
        self.assertEqual(sum(out),120)
        self.assertEqual(len(err),0) 

    def test_iter_apply(self):
        pipe = Pipeline([10,20, [30,40,50]]).apply(lambda x:x).iter_apply(lambda x:x*10)
        self.assertIsInstance(pipe, Pipeline)
        self.assertIsInstance(Pipeline([]).iter_apply(func=func), Pipeline)
        with self.assertRaises(ValueError):
            Pipeline([]).iter_apply(object())
        with self.assertRaises(TypeError):
            Pipeline([]).iter_apply(not_a_param=object())
        out, err = pipe.run()
        self.assertEqual(len(out),3)
        self.assertEqual(sum(out),1200)
        distinct_err = [*{type(i) for i in err}]
        self.assertEqual(len(err), 2)
        self.assertEqual(len(distinct_err), 1)
        self.assertEqual(distinct_err[0], TypeError)

    def test_threads(self):
        pipe_threads = Pipeline(range(1,3), threads=10).apply(sleep)
        threads_time = timeit("pipe_threads.run()", number=1, globals={"pipe_threads":pipe_threads})
        self.assertLess(threads_time, 3)
        pipe_no_threads = Pipeline(range(1,3), threads=1).apply(sleep)
        no_threads_time = timeit("pipe_no_threads.run()", number=1, globals={"pipe_no_threads":pipe_no_threads})
        self.assertGreater(no_threads_time, 3)
        

def run_tests():
    main(__name__)

if __name__ == "__main__":
    run_tests()
