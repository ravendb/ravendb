using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using Sparrow.Logging;
using Sparrow.Utils;
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

        private class EnvSyncReq
        {
            public StorageEnvironment Env;
            public long LastKnownSyncCounter;
        }

        private readonly ConcurrentQueue<StorageEnvironment> _maybeNeedToFlush = new ConcurrentQueue<StorageEnvironment>();
        private readonly ManualResetEventSlim _flushWriterEvent = new ManualResetEventSlim();
        private readonly int _lowNumberOfFlushingResources = Math.Max(StorageEnvironment.MaxConcurrentFlushes / 10, 3);
        private readonly SemaphoreSlim _concurrentFlushes = new SemaphoreSlim(StorageEnvironment.MaxConcurrentFlushes);
        private readonly HashSet<StorageEnvironment> _avoidDuplicates = new HashSet<StorageEnvironment>();
        private readonly ConcurrentQueue<EnvSyncReq> _maybeNeedToSync = new ConcurrentQueue<EnvSyncReq>();
        private readonly ConcurrentQueue<EnvSyncReq> _syncIsRequired = new ConcurrentQueue<EnvSyncReq>();
        private readonly ConcurrentDictionary<uint, MountPointInfo> _mountPoints = new ConcurrentDictionary<uint, MountPointInfo>();

        private readonly Logger _log = LoggingSource.Instance.GetLogger<GlobalFlushingBehavior>("Global Flusher");

        private class MountPointInfo
        {
            public readonly ConcurrentQueue<EnvSyncReq> StorageEnvironments = new ConcurrentQueue<EnvSyncReq>();
        }

        public bool HasLowNumberOfFlushingResources => _concurrentFlushes.CurrentCount <= _lowNumberOfFlushingResources;

        public void VoronEnvironmentFlushing()
        {
            // We want this to always run, even if we dispose / create new storage env, this is 
            // static for the life time of the process, and environments will register / unregister from
            // it as needed
            while (true)
            {
                _avoidDuplicates.Clear();
                var maybeNeedSync = _maybeNeedToSync.Count;
                var millisecondsTimeout = 15000 - maybeNeedSync;
                if (millisecondsTimeout <= 0 ||
                    _flushWriterEvent.Wait(millisecondsTimeout) == false)
                {
                    if (_maybeNeedToSync.Count == 0)
                        continue;

                    if (_log.IsInfoEnabled)
                    {
                        _log.Info($"Starting desired sync with {_maybeNeedToSync.Count:#,#} items to sync after {millisecondsTimeout:#,#} ms with no activity");
                    }

                    // sync after 5 seconds if no flushing occured, or if there has been a LOT of
                    // writes that we would like to run
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
            _avoidDuplicates.Clear();
            EnvSyncReq envToSync;
            var limit = _maybeNeedToSync.Count;
            while (
                // if there is high traffic into the queue, we want to abort after 
                // we processed whatever was already in there, to avoid holding up
                // the rest of the operations
                limit-- > 0 &&
                _maybeNeedToSync.TryDequeue(out envToSync))
            {
                if (_avoidDuplicates.Add(envToSync.Env) == false)
                    continue; // already seen

                if (envToSync.Env.Disposed)
                    continue;

                var mpi = _mountPoints.GetOrAdd(envToSync.Env.Options.DataPager.UniquePhysicalDriveId,
                    _ => new MountPointInfo());

                mpi.StorageEnvironments.Enqueue(envToSync);
            }

            foreach (var mountPoint in _mountPoints)
            {
                int parallelSyncsPerIo = Math.Min(StorageEnvironment.NumOfCocurrentSyncsPerPhysDrive, mountPoint.Value.StorageEnvironments.Count);

                for (int i = 0; i < parallelSyncsPerIo; i++)
                {
                    ThreadPool.QueueUserWorkItem(SyncAllEnvironmentsInMountPoint, mountPoint.Value); 
                }
            }
        }

        private void SyncRequiredEnvironments()
        {
            _avoidDuplicates.Clear();

            var limit = _syncIsRequired.Count;
            while (
                // if there is high traffic into the queue, we want to abort after 
                // we processed whatever was already in there, to avoid holding up
                // the rest of the operations
                limit-- > 0 &&
                _syncIsRequired.TryDequeue(out EnvSyncReq item))
            {
                if (_avoidDuplicates.Add(item.Env) == false)
                    continue; // avoid duplicates in batch

                if (ThreadPool.QueueUserWorkItem(state => SyncEnvironment((EnvSyncReq)state), item) == false)
                {
                    SyncEnvironment(item);
                }
            }
        }

        private void SyncAllEnvironmentsInMountPoint(object mt)
        {
            var mountPointInfo = (MountPointInfo)mt;
            EnvSyncReq req;
            while (mountPointInfo.StorageEnvironments.TryDequeue(out req))
            {
                SyncEnvironment(req);

                // we have mutliple threads racing for this value, no a concern, the last one wins is probably
                // going to be the latest, or close enough that we don't care
                Volatile.Write(ref req.Env.LastSyncTimeInTicks, DateTime.UtcNow.Ticks);
            }
        }

        private void SyncEnvironment(EnvSyncReq req)
        {
            if (req.Env.Disposed)
                return;

            if (req.Env.LastSyncCounter > req.LastKnownSyncCounter)
                return; // we already a sync after this was scheduled

            try
            {
                using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(req.Env.Journal.Applicator))
                {
                    operation.SyncDataFile();
                }
            }
            catch (Exception e)
            {
                if (_log.IsOperationsEnabled)
                    _log.Operations($"Failed to sync data file for {req.Env.Options.BasePath}", e);
                req.Env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
            }
        }


        private void FlushEnvironments()
        {
            _avoidDuplicates.Clear();
            StorageEnvironment envToFlush;
            var limit = _maybeNeedToFlush.Count;
            while (
                // if there is high traffic into the queue, we want to abort after 
                // we processed whatever was already in there, to avoid holding up
                // the rest of the operations
                limit-- > 0 &&
                _maybeNeedToFlush.TryDequeue(out envToFlush))
            {
                if (_avoidDuplicates.Add(envToFlush) == false)
                    continue; // avoid duplicates
                if (envToFlush.Disposed || envToFlush.Options.ManualFlushing)
                    continue;

                var sizeOfUnflushedTransactionsInJournalFile = Volatile.Read(ref envToFlush.SizeOfUnflushedTransactionsInJournalFile);

                if (sizeOfUnflushedTransactionsInJournalFile == 0)
                    continue; // nothing to do


                if (sizeOfUnflushedTransactionsInJournalFile < envToFlush.Options.MaxNumberOfPagesInJournalBeforeFlush)
                {
                    // we haven't reached the point where we have to flush, but we might want to, if we have enough 
                    // resources available, if we have more than half the flushing capacity, we can do it now, otherwise, we'll wait
                    // until it is actually required.
                    if (_concurrentFlushes.CurrentCount < StorageEnvironment.MaxConcurrentFlushes / 2)
                        continue;

                    // At the same time, we want to avoid excessive flushes, so we'll limit it to once in a while if we don't
                    // have a lot to flush
                    if ((DateTime.UtcNow - envToFlush.LastFlushTime).TotalSeconds < StorageEnvironment.TimeToSyncAfterFlashInSeconds)
                        continue;
                }

                envToFlush.LastFlushTime = DateTime.UtcNow;
                Interlocked.Add(ref envToFlush.SizeOfUnflushedTransactionsInJournalFile, -sizeOfUnflushedTransactionsInJournalFile);

                _concurrentFlushes.Wait();

                ThreadPool.QueueUserWorkItem(env =>
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
                        if (_log.IsOperationsEnabled)
                            _log.Operations($"Failed to flush {storageEnvironment.Options.BasePath}", e);

                        storageEnvironment.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                    }
                    finally
                    {
                        _concurrentFlushes.Release();
                    }
                }, envToFlush);
            }
        }

        public void MaybeFlushEnvironment(StorageEnvironment env)
        {
            if (env.Options.ManualFlushing)
                return;

            _maybeNeedToFlush.Enqueue(env);
            _flushWriterEvent.Set();
        }

        public void MaybeSyncEnvironment(StorageEnvironment env)
        {
            _maybeNeedToSync.Enqueue(new EnvSyncReq
            {
                Env = env,
                LastKnownSyncCounter = env.LastSyncCounter
            });
        }

        public void ForceFlushAndSyncEnvironment(StorageEnvironment env)
        {
            _syncIsRequired.Enqueue(new EnvSyncReq
            {
                Env = env,
                LastKnownSyncCounter = env.LastSyncCounter
            });
            _flushWriterEvent.Set();
        }
    }
}
