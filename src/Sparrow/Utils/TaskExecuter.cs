using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Utils
{
    /// <summary>
    /// Allow to raise a task completion source with minimal costs
    /// and attempt to avoid stalls due to thread pool starvation
    /// </summary>
    public static class TaskExecuter
    {
        private static readonly Runner Instance = new Runner();

        private class Runner
        {
            private readonly ConcurrentQueue<(WaitCallback,object)> _actions = new ConcurrentQueue<(WaitCallback, object)>();

            private readonly ManualResetEvent _event = new ManualResetEvent(false);

            private void Run()
            {
                while (true)
                {
                    (WaitCallback callback, object state) result;
                    while (_actions.TryDequeue(out result))
                    {
                        try
                        {
                            result.callback(result.state);
                        }
                        catch { }
                    }
                    _event.WaitOne();
                    _event.Reset();
                }
            }

            public void Enqueue(WaitCallback callback, object state)
            {
                _actions.Enqueue((callback, state));
                _event.Set();
            }

            public Runner()
            {
                new Thread(Run)
                {
                    IsBackground = true,
                    Name = "RavenDB Tasks Executer"
                }.Start();
            }
        }

        private static void TaskCompletionCallback(object state) => ((TaskCompletionSource<object>)state).TrySetResult(null);

        public static void CompleteAndReplace(ref TaskCompletionSource<object> task)
        {
            var task2 = Interlocked.Exchange(ref task, new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
            Execute(TaskCompletionCallback, task2);
        }

        public static void CompleteReplaceAndExecute(ref TaskCompletionSource<object> task, Action act)
        {
            var task2 = Interlocked.Exchange(ref task, new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
            Execute(state =>
            {
                var (tcs, action) = ((TaskCompletionSource<object>, Action))state;
                tcs.TrySetResult(null);
                act();
            }, (task2, act));
        }

        public static void Complete(TaskCompletionSource<object> task)
        {
            Execute(TaskCompletionCallback, task);
        }

        private class RunOnce
        {
            private WaitCallback _callback;

            public RunOnce(WaitCallback callback)
            {
                _callback = callback;
            }

            public void Execute(object state)
            {
                var callback = _callback;
                if (callback == null)
                    return;

                if(Interlocked.CompareExchange(ref _callback, null, callback) != callback)
                    return;

                callback(state);
            }
        }

        public static void Execute(WaitCallback callback, object state)
        {
            callback = new RunOnce(callback).Execute;
            Instance.Enqueue(callback, state);
            ThreadPool.QueueUserWorkItem(callback, state);
        }
    }
}