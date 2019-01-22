using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Utils;
using Voron.Impl.Journal;

namespace Voron
{
    public class GlobalFlushingBehavior
    {
        private const string FlushingThreadName = "Voron Global Flushing Thread";
        
        internal static readonly Lazy<GlobalFlushingBehavior> GlobalFlusher = new Lazy<GlobalFlushingBehavior>(() =>
        {
            var flusher = new GlobalFlushingBehavior();
            var thread = new Thread(flusher.VoronEnvironmentFlushing)
            {
                IsBackground = true,
                Name = FlushingThreadName
            };
            thread.Start();
            return flusher;
        });

        private class EnvSyncReq
        {
            public StorageEnvironment Env => Reference?.Owner;
            public StorageEnvironment.IndirectReference Reference;
            public long LastKnownSyncCounter;
        }

        private readonly ManualResetEventSlim _flushWriterEvent = new ManualResetEventSlim();
        private readonly int _lowNumberOfFlushingResources = Math.Max(StorageEnvironment.MaxConcurrentFlushes / 10, 3);
        private readonly SemaphoreSlim _concurrentFlushes = new SemaphoreSlim(StorageEnvironment.MaxConcurrentFlushes);

        private readonly ConcurrentQueue<EnvSyncReq> _maybeNeedToFlush = new ConcurrentQueue<EnvSyncReq>();
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
            NativeMemory.EnsureRegistered();
            // We want this to always run, even if we dispose / create new storage env, this is 
            // static for the life time of the process, and environments will register / unregister from
            // it as needed

            try
            {
                var avoidDuplicates = new HashSet<StorageEnvironment>();
                while (true)
                {
                    avoidDuplicates.Clear();
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

                        // sync after 5 seconds if no flushing occurred, or if there has been a LOT of
                        // writes that we would like to run
                        SyncDesiredEnvironments(avoidDuplicates);
                        continue;
                    }
                    _flushWriterEvent.Reset();

                    FlushEnvironments(avoidDuplicates);

                    SyncRequiredEnvironments(avoidDuplicates);
                }
            }
            catch (Exception e)
            {
                if (_log.IsOperationsEnabled)
                {
                    _log.Operations("Catastrophic failure in Voron environment flushing", e);
                }

                // wait for the message to be flushed to the logs
                Thread.Sleep(5000);

                // Note that we intentionally don't have error handling here.
                // If this code throws an exception that bubbles up to here, we WANT the process
                // to die, since we can't recover from the flusher thread dying.
                throw;
            }
            // ReSharper disable once FunctionNeverReturns
        }

        private void SyncDesiredEnvironments(HashSet<StorageEnvironment> avoidDuplicates)
        {
            avoidDuplicates.Clear();
            EnvSyncReq envToSync;
            var limit = _maybeNeedToSync.Count;
            while (
                // if there is high traffic into the queue, we want to abort after 
                // we processed whatever was already in there, to avoid holding up
                // the rest of the operations
                limit-- > 0 &&
                _maybeNeedToSync.TryDequeue(out envToSync))
            {
                var storageEnvironment = envToSync.Env;
                if (storageEnvironment == null)
                    continue;

                if (avoidDuplicates.Add(storageEnvironment) == false)
                    continue; // already seen

                if (storageEnvironment.Disposed)
                    continue;

                var mpi = _mountPoints.GetOrAdd(storageEnvironment.Options.DataPager.UniquePhysicalDriveId,
                    _ => new MountPointInfo());

                mpi.StorageEnvironments.Enqueue(envToSync);
            }

            foreach (var mountPoint in _mountPoints)
            {
                int parallelSyncsPerIo = Math.Min(StorageEnvironment.NumOfConcurrentSyncsPerPhysDrive, mountPoint.Value.StorageEnvironments.Count);

                for (int i = 0; i < parallelSyncsPerIo; i++)
                {
                    ThreadPool.QueueUserWorkItem(SyncAllEnvironmentsInMountPoint, mountPoint.Value); 
                }
            }
        }

        private void SyncRequiredEnvironments(HashSet<StorageEnvironment> avoidDuplicates)
        {
            avoidDuplicates.Clear();

            var limit = _syncIsRequired.Count;
            while (
                // if there is high traffic into the queue, we want to abort after 
                // we processed whatever was already in there, to avoid holding up
                // the rest of the operations
                limit-- > 0 &&
                _syncIsRequired.TryDequeue(out EnvSyncReq item))
            {
                var storageEnvironment = item.Env;
                if(storageEnvironment == null)
                    continue;
                if (avoidDuplicates.Add(storageEnvironment) == false)
                    continue; // avoid duplicates in batch

                ThreadPool.QueueUserWorkItem(state => SyncEnvironment((EnvSyncReq)state), item);
            }
        }

        private void SyncAllEnvironmentsInMountPoint(object mt)
        {
            var mountPointInfo = (MountPointInfo)mt;
            EnvSyncReq req;
            while (mountPointInfo.StorageEnvironments.TryDequeue(out req))
            {
                SyncEnvironment(req);

                // we have multiple threads racing for this value, no a concern, the last one wins is probably
                // going to be the latest, or close enough that we don't care
                var storageEnvironment = req.Env;
                if(storageEnvironment != null)
                    Interlocked.Exchange(ref storageEnvironment.LastSyncTimeInTicks, DateTime.UtcNow.Ticks);
            }
        }

        private void SyncEnvironment(EnvSyncReq req)
        {
            var storageEnvironment = req.Env;
            if (storageEnvironment  == null || storageEnvironment.Disposed)
                return;

            if (storageEnvironment.LastSyncCounter > req.LastKnownSyncCounter)
                return; // we already a sync after this was scheduled

            try
            {
                using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(storageEnvironment.Journal.Applicator))
                {
                    operation.SyncDataFile();
                }
            }
            catch (Exception e)
            {
                if (_log.IsOperationsEnabled)
                    _log.Operations($"Failed to sync data file for {storageEnvironment.Options.BasePath}", e);
                storageEnvironment.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
            }
        }


        private void FlushEnvironments(HashSet<StorageEnvironment> avoidDuplicates)
        {
            avoidDuplicates.Clear();
            EnvSyncReq req;
            var limit = _maybeNeedToFlush.Count;
            while (
                // if there is high traffic into the queue, we want to abort after 
                // we processed whatever was already in there, to avoid holding up
                // the rest of the operations
                limit-- > 0 &&
                _maybeNeedToFlush.TryDequeue(out req))
            {
                var envToFlush = req.Env;
                if (envToFlush == null)
                    continue;
                if (avoidDuplicates.Add(envToFlush) == false)
                    continue; // avoid duplicates
                if (envToFlush.Disposed || envToFlush.Options.ManualFlushing)
                    continue;

                var sizeOfUnflushedTransactionsInJournalFile = envToFlush.SizeOfUnflushedTransactionsInJournalFile;

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
                    if ((DateTime.UtcNow - envToFlush.LastFlushTime).TotalSeconds < StorageEnvironment.TimeToSyncAfterFlashInSec)
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

            _maybeNeedToFlush.Enqueue(new EnvSyncReq
            {
                Reference = env.SelfReference,
            });
            _flushWriterEvent.Set();
        }

        public void MaybeSyncEnvironment(StorageEnvironment env)
        {
            if (env.Options.ManualSyncing)
                return;

            _maybeNeedToSync.Enqueue(new EnvSyncReq
            {
                Reference = env.SelfReference,
                LastKnownSyncCounter = env.LastSyncCounter
            });
        }

        public void ForceFlushAndSyncEnvironment(StorageEnvironment env)
        {
            _syncIsRequired.Enqueue(new EnvSyncReq
            {
                Reference = env.SelfReference,
                LastKnownSyncCounter = env.LastSyncCounter
            });
            _flushWriterEvent.Set();
        }
    }
}
