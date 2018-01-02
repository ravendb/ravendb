// -----------------------------------------------------------------------
//  <copyright file="AsyncHelpers.cs" company="Hibernating Rhinos LTD">
//      Copyright (coffee) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Util
{
    public static class AsyncHelpers
    {
        public static void RunSync(Func<Task> task)
        {
            var sw = Stopwatch.StartNew();
            var oldContext = SynchronizationContext.Current;
            try
            {
                var synch = new ExclusiveSynchronizationContext();
                SynchronizationContext.SetSynchronizationContext(synch);
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
        }

        public static T RunSync<T>(Func<Task<T>> task)
        {
            var result = default(T);
            var sw = Stopwatch.StartNew();
            var oldContext = SynchronizationContext.Current;
            try
            {
                var synch = new ExclusiveSynchronizationContext();
                SynchronizationContext.SetSynchronizationContext(synch);

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

            return result;
        }

        private class ExclusiveSynchronizationContext : SynchronizationContext
        {
            private readonly AutoResetEvent workItemsWaiting = new AutoResetEvent(false);
            private readonly Queue<Tuple<SendOrPostCallback, object>> items = new Queue<Tuple<SendOrPostCallback, object>>();

            private bool done;
            public Exception InnerException { get; set; }

            public override void Send(SendOrPostCallback d, object state)
            {
                throw new NotSupportedException("We cannot send to our same thread");
            }

            public override void Post(SendOrPostCallback d, object state)
            {
                lock (items)
                {
                    items.Enqueue(Tuple.Create(d, state));
                }
                workItemsWaiting.Set();
            }

            public void EndMessageLoop()
            {
                Post(_ => done = true, null);
            }

            public void BeginMessageLoop()
            {
                while (!done)
                {
                    Tuple<SendOrPostCallback, object> task = null;
                    lock (items)
                    {
                        if (items.Count > 0)
                        {
                            task = items.Dequeue();
                        }
                    }
                    if (task != null)
                    {
                        task.Item1(task.Item2);
                        if (InnerException != null) // the method threw an exeption
                        {
                            throw new AggregateException("AsyncHelpers.Run method threw an exception.", InnerException);
                        }
                    }
                    else
                    {
                        workItemsWaiting.WaitOne();
                    }
                }
            }

            public override SynchronizationContext CreateCopy()
            {
                return this;
            }
        }
    }
}
