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
    public static class TaskExecutor
    {
        private static readonly Runner Instance = new Runner();

        private class Runner
        {
            private const string TasksExecuterThreadName = "RavenDB Tasks Executer";
            private readonly ConcurrentQueue<(WaitCallback,object)> _actions = new ConcurrentQueue<(WaitCallback, object)>();

            private readonly ManualResetEventSlim _event = new ManualResetEventSlim(false);

            private void Run()
            {
                NativeMemory.EnsureRegistered();
                
                int tries = 0;
                while (true)
                {                    
                    (WaitCallback callback, object state) result;
                    while (_actions.TryDequeue(out result))
                    {
                        try
                        {
                            result.callback(result.state);
                        }
                        catch
                        {
                            // there is nothing that we _can_ do here that would be right
                            // and there is no meaningful error handling. Ignoring this because
                            // callers are expected to do their own exception catching
                        }
                    }

                    // PERF: Entering a kernel lock even if the ManualResetEventSlim will try to avoid that doing some spin locking
                    //       is very costly. This is a hack that is allowing amortize a bit very high frequency events. The proper
                    //       way to handle requires infrastructure changes. http://issues.hibernatingrhinos.com/issue/RavenDB-8126
                    if (tries < 5)
                    {
                        // Yield execution quantum. If we are in a high-frequency event we will be able to avoid the kernel lock. 
                        Thread.Sleep(0);
                        tries++;
                    }
                    else
                    {
                        _event.WaitHandle.WaitOne();
                        _event.Reset();
                        
                        // Nothing we can do here, just block.
                        tries = 0;
                    }
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
                    Name = TasksExecuterThreadName
                }.Start();
            }
        }

        public static void CompleteAndReplace(ref TaskCompletionSource<object> task)
        {
            var task2 = Interlocked.Exchange(ref task, new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
            task2.TrySetResult(null);
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
            task.TrySetResult(null);
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
