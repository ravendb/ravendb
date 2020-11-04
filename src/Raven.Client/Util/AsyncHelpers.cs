// -----------------------------------------------------------------------
//  <copyright file="AsyncHelpers.cs" company="Hibernating Rhinos LTD">
//      Copyright (coffee) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Sparrow;

namespace Raven.Client.Util
{
    public static class AsyncHelpers
    {
        public static bool UseTaskAwaiterWhenNoSynchronizationContextIsAvailable = true;

        private struct ExclusiveSynchronizationContextResetBehavior : IResetSupport<ExclusiveSynchronizationContext>
        {
            public void Reset(ExclusiveSynchronizationContext value)
            {
                value.Reset();
            }
        }

        private static ObjectPool<ExclusiveSynchronizationContext, ExclusiveSynchronizationContextResetBehavior> _pool = new ObjectPool<ExclusiveSynchronizationContext, ExclusiveSynchronizationContextResetBehavior>(() => new ExclusiveSynchronizationContext(), 10);

        public static void RunSync(Func<Task> task)
        {
            var oldContext = SynchronizationContext.Current;

            // Do we have an active synchronization context?
            if (UseTaskAwaiterWhenNoSynchronizationContextIsAvailable && oldContext == null)
            {
                // We can run synchronously without any issue.
                task().GetAwaiter().GetResult();
                return;
            }

            var sw = Stopwatch.StartNew();
            var synch = _pool.Allocate();

            SynchronizationContext.SetSynchronizationContext(synch);
            try
            {
                synch.Post(async _ =>
                {
                    try
                    {
                        await task().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        synch.InnerException = e;
                        throw;
                    }
                    finally
                    {
                        synch.EndMessageLoop();
                    }
                }, null);
                synch.BeginMessageLoop();
            }
            catch (AggregateException ex)
            {
                var exception = ex.ExtractSingleInnerException();
                if (exception is OperationCanceledException)
                    throw new TimeoutException("Operation timed out after: " + sw.Elapsed, ex);
                ExceptionDispatchInfo.Capture(exception).Throw();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(oldContext);
            }

            _pool.Free(synch);
        }

        public static T RunSync<T>(Func<Task<T>> task)
        {
            var oldContext = SynchronizationContext.Current;

            // Do we have an active synchronization context?
            if (UseTaskAwaiterWhenNoSynchronizationContextIsAvailable && oldContext == null)
            {
                // We can run synchronously without any issue.
                return task().GetAwaiter().GetResult();
            }

            var result = default(T);

            var sw = Stopwatch.StartNew();
            var synch = _pool.Allocate();

            SynchronizationContext.SetSynchronizationContext(synch);
            try
            {
                synch.Post(async _ =>
                {
                    try
                    {
                        result = await task().ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        synch.InnerException = e;
                        throw;
                    }
                    finally
                    {
                        sw.Stop();
                        synch.EndMessageLoop();
                    }
                }, null);
                synch.BeginMessageLoop();
            }
            catch (AggregateException ex)
            {
                var exception = ex.ExtractSingleInnerException();
                if (exception is OperationCanceledException)
                    throw new TimeoutException("Operation timed out after: " + sw.Elapsed, ex);
                ExceptionDispatchInfo.Capture(exception).Throw();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(oldContext);
            }

            _pool.Free(synch);

            return result;
        }

        private class ExclusiveSynchronizationContext : SynchronizationContext
        {
            private readonly AutoResetEvent _workItemsWaiting = new AutoResetEvent(false);
            private readonly ConcurrentQueue<Tuple<SendOrPostCallback, object>> _items = new ConcurrentQueue<Tuple<SendOrPostCallback, object>>();

            private bool _done;
            public Exception InnerException { get; set; }

            public override void Send(SendOrPostCallback d, object state)
            {
                throw new NotSupportedException("We cannot send to our same thread");
            }

            public override void Post(SendOrPostCallback d, object state)
            {
                _items.Enqueue(Tuple.Create(d, state));
                _workItemsWaiting.Set();
            }

            public void Reset()
            {
                _workItemsWaiting.Reset();
                _done = false;
                InnerException = null;
                while (_items.TryDequeue(out var dummy))
                {
                    // Drain queue in case of exceptions.
                }
            }

            public void EndMessageLoop()
            {
                Post(_ => _done = true, null);
            }

            public void BeginMessageLoop()
            {
                OperationStarted();

                // Start to process in a loop
                while (!_done)
                {
                    // If the queue is empty, we wait.
                    if (_items.IsEmpty)
                        _workItemsWaiting.WaitOne();

                    // Queue is no longer empty (unless someone won) therefore we are ready to process.
                    while (_items.TryDequeue(out var task))
                    {
                        // Execute the operation.
                        task.Item1(task.Item2);
                        if (InnerException != null) // the method threw an exception
                        {
                            throw new AggregateException("AsyncHelpers.Run method threw an exception.", InnerException);
                        }
                    }
                }

                OperationCompleted();
            }

            public override SynchronizationContext CreateCopy()
            {
                return this;
            }
        }
    }
}
