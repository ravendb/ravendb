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
        private const string FlushingThreadName = "Voron Global Flushing Thread";

        public static int NumberOfConcurrentSyncsPerPhysicalDrive = 3;

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
            public int IsSyncRun;
            public StorageEnvironment.IndirectReference Reference;
            public bool IsRequired;
        }

        private readonly ManualResetEventSlim _flushWriterEvent = new ManualResetEventSlim();
        private readonly int _lowNumberOfFlushingResources = Math.Max(StorageEnvironment.MaxConcurrentFlushes / 10, 3);
        private readonly SemaphoreSlim _concurrentFlushesAvailable = new SemaphoreSlim(StorageEnvironment.MaxConcurrentFlushes);

        private readonly ConcurrentQueue<EnvSyncReq> _maybeNeedToFlush = new ConcurrentQueue<EnvSyncReq>();

        private readonly ConcurrentDictionary<StorageEnvironment.IndirectReference, EnvSyncReq> _envsToSync = new ConcurrentDictionary<StorageEnvironment.IndirectReference, EnvSyncReq>();

        private readonly ConcurrentDictionary<uint, MountPointInfo> _mountPoints = new ConcurrentDictionary<uint, MountPointInfo>();

        private readonly Logger _log = LoggingSource.Instance.GetLogger<GlobalFlushingBehavior>("Global Flusher");

        private class MountPointInfo
        {
            public readonly ConcurrentQueue<EnvSyncReq> StorageEnvironments = new ConcurrentQueue<EnvSyncReq>();
        }

        public bool HasLowNumberOfFlushingResources => _concurrentFlushesAvailable.CurrentCount <= _lowNumberOfFlushingResources;

        private void VoronEnvironmentFlushing()
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

                    if (_flushWriterEvent.Wait(5000) == false)
                    {
                        if (_envsToSync.Count == 0)
                            continue;

                        if (_log.IsInfoEnabled)
                        {
                            _log.Info($"Starting force sync with {_envsToSync.Count:#,#} items to sync after a period of no activity");
                        }

                        // sync after 5 seconds if no flushing occurred
                        SyncEnvironments(force: true);
                        continue;
                    }
                    _flushWriterEvent.Reset();

                    FlushEnvironments(avoidDuplicates);

                    SyncEnvironments(force: false);
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

        private void SyncEnvironments(bool force)
        {
            foreach (var envSyncReq in _envsToSync)
            {
                var env = envSyncReq.Key.Owner;
                if (env == null)
                {
                    _envsToSync.TryRemove(envSyncReq.Key, out _);
                    continue;
                }

                if (env.Disposed)
                    continue;

                if (force == false && envSyncReq.Value.IsRequired == false &&
                   env.Journal.Files.Count + env.Journal.Applicator.JournalsToDeleteCount <= env.Options.SyncJournalsCountThreshold)
                    continue;

                var isSyncRun = Interlocked.CompareExchange(ref envSyncReq.Value.IsSyncRun, 1, 0);
                if (isSyncRun != 0)
                    continue;

                var mpi = _mountPoints.GetOrAdd(env.Options.DataPager.UniquePhysicalDriveId,
                    _ => new MountPointInfo());

                mpi.StorageEnvironments.Enqueue(envSyncReq.Value);
            }

            foreach (var mountPoint in _mountPoints)
            {
                int parallelSyncsPerIo = Math.Min(NumberOfConcurrentSyncsPerPhysicalDrive, mountPoint.Value.StorageEnvironments.Count);

                for (int i = 0; i < parallelSyncsPerIo; i++)
                {
                    ThreadPool.QueueUserWorkItem(SyncAllEnvironmentsInMountPoint, mountPoint.Value);
                }
            }
        }

        private void SyncAllEnvironmentsInMountPoint(object mt)
        {
            var mountPointInfo = (MountPointInfo)mt;
            while (mountPointInfo.StorageEnvironments.TryDequeue(out var req))
            {
                SyncEnvironment(req);
                _envsToSync.TryRemove(req.Reference, out _);
            }
        }

        private void SyncEnvironment(EnvSyncReq req)
        {
            var storageEnvironment = req.Env;
            if (storageEnvironment == null || storageEnvironment.Disposed || storageEnvironment.Options.ManualSyncing)
                return;

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
                limit > 0 &&
                _maybeNeedToFlush.TryDequeue(out req))
            {
                var envToFlush = req.Env;
                if (envToFlush == null)
                    continue;
                if (avoidDuplicates.Add(envToFlush) == false)
                    continue; // avoid duplicates
                if (envToFlush.Disposed || envToFlush.Options.ManualFlushing)
                {
                    if (_forTestingPurposes == null || _forTestingPurposes.AllowToFlushEvenIfManualFlushingSet.Contains(envToFlush) == false)
                    {
                        continue;
                    }
                }

                var numberOfNewPagesSinceLastFlush = envToFlush.Journal.Applicator.TotalCommittedSinceLastFlushPages;

                if (envToFlush.Journal.Applicator.ShouldFlush == false)
                    continue; // nothing to do

                if (numberOfNewPagesSinceLastFlush < envToFlush.Options.MaxNumberOfPagesInJournalBeforeFlush)
                {
                    // we haven't reached the point where we have to flush, but we might want to, if we have enough 
                    // resources available, if we have more than half the flushing capacity, we can do it now, otherwise, we'll wait
                    // until it is actually required.
                    if (_concurrentFlushesAvailable.CurrentCount < StorageEnvironment.MaxConcurrentFlushes / 2)
                        continue;

                    // At the same time, we want to avoid excessive flushes, so we'll limit it to once in a while if we don't
                    // have a lot to flush
                    if ((DateTime.UtcNow - envToFlush.LastFlushTime).TotalSeconds < envToFlush.TimeToSyncAfterFlushInSec)
                        continue;
                }

                envToFlush.LastFlushTime = DateTime.UtcNow;

                _concurrentFlushesAvailable.Wait();
                limit--;

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
                        _concurrentFlushesAvailable.Release();
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

        public void SuggestSyncEnvironment(StorageEnvironment env)
        {
            AddEnvironmentSyncRequest(env, false);
        }

        private void AddEnvironmentSyncRequest(StorageEnvironment env, bool required)
        {
            while (true)
            {
                if (_envsToSync.TryGetValue(env.SelfReference, out var syncReq))
                {
                    syncReq.IsRequired = required;
                    break;
                }

                var added = _envsToSync.TryAdd(env.SelfReference, new EnvSyncReq
                {
                    Reference = env.SelfReference,
                    IsRequired = required
                });

                if (added)
                {
                    break;
                }
            }
        }

        public void ForceSyncEnvironment(StorageEnvironment env)
        {
            AddEnvironmentSyncRequest(env, true);
            _flushWriterEvent.Set();
        }

        private TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff(this);
        }

        internal class TestingStuff
        {
            internal readonly List<StorageEnvironment> AllowToFlushEvenIfManualFlushingSet = new List<StorageEnvironment>();

            private GlobalFlushingBehavior _parent;

            public TestingStuff(GlobalFlushingBehavior globalFlushingBehavior)
            {
                _parent = globalFlushingBehavior;
            }

            internal void AddEnvironmentToFlushQueue(StorageEnvironment env)
            {
                _parent._maybeNeedToFlush.Enqueue(new EnvSyncReq
                {
                    Reference = env.SelfReference,
                });
            }

            internal void ForceFlushEnvironment()
            {
                _parent.FlushEnvironments(new HashSet<StorageEnvironment>());
            }
        }
    }
}
