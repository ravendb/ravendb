using System;
using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Voron
{
    public class GlobalFlushingBehavior
    {
        private readonly ConcurrentQueue<StorageEnvironment> _maybeNeedToFlush = new ConcurrentQueue<StorageEnvironment>();
        private readonly ManualResetEventSlim _flushWriterEvent = new ManualResetEventSlim();
        private readonly SemaphoreSlim _concurrentFlushes = new SemaphoreSlim(StorageEnvironment.MaxConcurrentFlushes);

        public void VoronEnvironmentFlushing()
        {
            // We want this to always run, even if we dispose / create new storage env, this is 
            // static for the life time of the process, and environments will register / unregister from
            // it as needed
            while (true)
            {
                _flushWriterEvent.Wait();
                _flushWriterEvent.Reset();

                StorageEnvironment envToFlush;
                while (_maybeNeedToFlush.TryDequeue(out envToFlush))
                {
                    if (envToFlush.Disposed || envToFlush.Options.ManualFlushing)
                        continue;

                    var sizeOfUnflushedTransactionsInJournalFile = Volatile.Read(ref envToFlush.SizeOfUnflushedTransactionsInJournalFile);

                    if (sizeOfUnflushedTransactionsInJournalFile == 0)
                        continue; // nothing to do


                    if (sizeOfUnflushedTransactionsInJournalFile <
                        envToFlush.Options.MaxNumberOfPagesInJournalBeforeFlush)
                    {
                        // we haven't reached the point where we have to flush, but we might want to, if we have enough 
                        // resources available, if we have more than half the flushing capacity, we can do it now, otherwise, we'll wait
                        // until it is actually required.
                        if (_concurrentFlushes.CurrentCount < StorageEnvironment.MaxConcurrentFlushes / 2)
                            continue;
                    }

                    Interlocked.Add(ref envToFlush.SizeOfUnflushedTransactionsInJournalFile, -sizeOfUnflushedTransactionsInJournalFile);

                    _concurrentFlushes.Wait();

                    if (ThreadPool.QueueUserWorkItem(env =>
                    {
                        var storageEnvironment = ((StorageEnvironment)env);
                        try
                        {
                            if (storageEnvironment.Disposed)
                                return;
                            storageEnvironment.BackgroundFlushWritesToDataFile();
                        }
                        catch (Exception e)
                        {
                            storageEnvironment.FlushingTaskFailure = ExceptionDispatchInfo.Capture(e.InnerException);
                        }
                        finally
                        {
                            _concurrentFlushes.Release();
                        }
                    }, envToFlush) == false)
                    {
                        _concurrentFlushes.Release();
                        MaybeFlushEnvironment(envToFlush);// re-register if the thread pool is full
                        Thread.Sleep(0); // but let it give up the execution slice so we'll let the TP time to run
                    }
                }
            }
            // ReSharper disable once FunctionNeverReturns
        }


        public void MaybeFlushEnvironment(StorageEnvironment env)
        {
            _maybeNeedToFlush.Enqueue(env);
            _flushWriterEvent.Set();
        }
    }
}
