using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using Voron.Impl.Journal;

namespace Voron
{
    public class GlobalFlushingBehavior
    {
        internal static readonly Lazy<GlobalFlushingBehavior> GlobalFlusher = new Lazy<GlobalFlushingBehavior>(() =>
        {
            var flusher = new GlobalFlushingBehavior();
            var thread = new Thread(flusher.VoronEnvironmentFlushing)
            {
                IsBackground = true,
                Name = "Voron Global Flushing Thread"
            };
            thread.Start();
            return flusher;
        });

        private readonly ConcurrentQueue<StorageEnvironment> _maybeNeedToFlush = new ConcurrentQueue<StorageEnvironment>();
        private readonly ManualResetEventSlim _flushWriterEvent = new ManualResetEventSlim();
        private readonly SemaphoreSlim _concurrentFlushes = new SemaphoreSlim(StorageEnvironment.MaxConcurrentFlushes);

        private readonly ConcurrentQueue<StorageEnvironment> _maybeNeedToSync = new ConcurrentQueue<StorageEnvironment>();
        private readonly ConcurrentQueue<StorageEnvironment> _syncIsRequired = new ConcurrentQueue<StorageEnvironment>();
        private readonly ConcurrentDictionary<uint, MountPointInfo> _mountPoints = new ConcurrentDictionary<uint, MountPointInfo>();

        private class MountPointInfo
        {
            public readonly ConcurrentQueue<StorageEnvironment> StorageEnvironments = new ConcurrentQueue<StorageEnvironment>();
            public DateTime LastSyncTimeInMountPoint = DateTime.MinValue;
        }

        public void VoronEnvironmentFlushing()
        {
            // We want this to always run, even if we dispose / create new storage env, this is 
            // static for the life time of the process, and environments will register / unregister from
            // it as needed
            while (true)
            {
                if (_flushWriterEvent.Wait(5000) == false)
                {
                    // sync after 5 seconds if no flushing occured
                    SyncDesiredEnvironments();
                    continue;
                }
                _flushWriterEvent.Reset();

                FlushEnvironments();

                SyncRequiredEnvironments();
            }
            // ReSharper disable once FunctionNeverReturns

            // Note that we intentionally don't have error handling here.
            // If this code throw an exception that bubbles up to here, we WANT the process
            // to die, since we can't recover from the flusher thread dying.
        }

        private void SyncDesiredEnvironments()
        {
            StorageEnvironment envToSync;
            while (_maybeNeedToSync.TryDequeue(out envToSync))
            {
                if (envToSync.Disposed)
                    continue;

                if (envToSync.IsDataFileEnqueuedToSync)
                    continue;

                envToSync.IsDataFileEnqueuedToSync = true;

                var mpi = _mountPoints.GetOrAdd(envToSync.Options.DataPager.UniquePhysicalDriveId,
                    _ => new MountPointInfo());

                mpi.StorageEnvironments.Enqueue(envToSync);
            }

            foreach (var mountPoint in _mountPoints)
            {
                if (DateTime.UtcNow - mountPoint.Value.LastSyncTimeInMountPoint > TimeSpan.FromMinutes(1))
                {
                    int parallelSyncsPerIo = 3;
                    parallelSyncsPerIo = Math.Min(parallelSyncsPerIo, mountPoint.Value.StorageEnvironments.Count);

                    for (int i = 0; i < parallelSyncsPerIo; i++)
                    {
                        if (ThreadPool.QueueUserWorkItem(SyncAllEnvironmentsInMountPoint, mountPoint.Value) == false)
                        {
                            SyncAllEnvironmentsInMountPoint(mountPoint.Value);
                        }
                    }
                }
            }
        }

        private void SyncRequiredEnvironments()
        {
            StorageEnvironment envToSync;
            while (_syncIsRequired.TryDequeue(out envToSync))
            {
                if (ThreadPool.QueueUserWorkItem(SyncEnvironment, envToSync) == false)
                {
                    // if threadpool queue is full - sync in this thread
                    SyncEnvironment(envToSync);
                }
            }
        }

        private void SyncEnvironment(object state)
        {
            var env = (StorageEnvironment)state;

            if (env.Disposed)
                return;

            var applicator = env.Journal.Applicator;

            long lastFlushedJournalCopy;
            Dictionary<long, JournalFile> journalsToDeleteCopy; 
            JournalFile lastFlushedJournalObjectCopy;
            long oldestActiveTransactionCopy;
            lock (applicator.LastFlushedLocker)
            {
                lastFlushedJournalCopy = applicator.LastFlushedJournal;
                journalsToDeleteCopy = new Dictionary<long, JournalFile>(applicator.JournalsToDelete);
                lastFlushedJournalObjectCopy = applicator.LastFlushedJournalObject;
                oldestActiveTransactionCopy = applicator.OldestActiveTransactionWhenFlushed;
            }

            var synced = applicator.SyncDataFile(oldestActiveTransactionCopy, journalsToDeleteCopy, lastFlushedJournalObjectCopy);
            if (synced == false)
            {
                // already syncing this environment. enque for later sync
                _maybeNeedToSync.Enqueue(env);
                return;
            }

            applicator.LastSyncedJournal = lastFlushedJournalCopy;
        }

        private void SyncAllEnvironmentsInMountPoint(object mt)
        {
            // TODO: Error handling
            var mountPointInfo = (MountPointInfo)mt;
            StorageEnvironment env;
            while (mountPointInfo.StorageEnvironments.TryDequeue(out env))
            {
                SyncEnvironment(env);
            }
            mountPointInfo.LastSyncTimeInMountPoint = DateTime.UtcNow;
        }

        private void FlushEnvironments()
        {
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

        public void MaybeFlushEnvironment(StorageEnvironment env)
        {
            _maybeNeedToFlush.Enqueue(env);
            _flushWriterEvent.Set();
        }

        public void MaybeSyncEnvironment(StorageEnvironment env)
        {
            _maybeNeedToSync.Enqueue(env);
        }

        public void ForceFlushAndSyncEnvironment(StorageEnvironment env)
        {
            _syncIsRequired.Enqueue(env);
            _flushWriterEvent.Set();
        }
    }
}
