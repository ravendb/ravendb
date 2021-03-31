using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Actions;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Notifications;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Search;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Storage.Voron;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.FileSystem.Synchronization.Conflictuality;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper.Unmanaged;
using Raven.Database.Impl;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Connections;
using Raven.Database.Util;
using Raven.Abstractions;
using Raven.Database.Common;

namespace Raven.Database.FileSystem
{
    public class RavenFileSystem : IResourceStore, IDisposable
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private readonly ConflictArtifactManager conflictArtifactManager;
        private readonly ConflictDetector conflictDetector;
        private readonly ConflictResolver conflictResolver;
        private readonly FileLockManager fileLockManager;	
        private readonly NotificationPublisher notificationPublisher;
        private readonly IndexStorage search;
        private readonly SigGenerator sigGenerator;
        private readonly ITransactionalStorage storage;
        private readonly SynchronizationTask synchronizationTask;
        private readonly InMemoryRavenConfiguration configuration;
        private readonly TransportState transportState;
        private readonly MetricsCountersManager metricsCounters;

        private readonly ThreadLocal<DisableTriggerState> disableAllTriggers = new ThreadLocal<DisableTriggerState>(() => new DisableTriggerState{Disabled = false});

        private volatile bool disposed;

        private Historian historian;

        public string Name { get; private set; }

        public string ResourceName { get; private set; }

        public RavenFileSystem(InMemoryRavenConfiguration config, string name, TransportState receivedTransportState = null)
        {
            ExtensionsState = new AtomicDictionary<object>();

            Name = name;
            
            ResourceName = string.Concat(Constants.FileSystem.UrlPrefix, "/", name);
            configuration = config;

            try
            {
                ValidateStorage();

                configuration.Container.SatisfyImportsOnce(this);

                transportState = receivedTransportState ?? new TransportState();

                storage = CreateTransactionalStorage(configuration);

                if (config.FileSystem.DisableRDC == false)
                    sigGenerator = new SigGenerator();

                fileLockManager = new FileLockManager();			        
   
                BufferPool = new BufferPool(1024 * 1024 * 1024, 65 * 1024);
                conflictDetector = new ConflictDetector();
                conflictResolver = new ConflictResolver(storage, new CompositionContainer(configuration.Catalog));

                notificationPublisher = new NotificationPublisher(transportState);
                synchronizationTask = new SynchronizationTask(storage, sigGenerator, notificationPublisher, configuration);

                FileLock = new FileSynchronizationLock(synchronizationTask, config);

                metricsCounters = new MetricsCountersManager();

                search = new IndexStorage(name, configuration);

                conflictArtifactManager = new ConflictArtifactManager(storage, search, this);

                TimerManager = new ResourceTimerManager();

                Tasks = new TaskActions(this, Log);
                Files = new FileActions(this, Log);
                Synchronizations = new SynchronizationActions(this, Log);

                AppDomain.CurrentDomain.ProcessExit += ShouldDispose;
                AppDomain.CurrentDomain.DomainUnload += ShouldDispose;
            }
            catch (Exception e)
            {
                Log.ErrorException(string.Format("Could not create file system '{0}'", Name ?? "unknown name"), e);
                try
                {
                    Dispose();
                }
                catch (Exception ex)
                {
                    Log.FatalException("Failed to dispose when already getting an error in file system ctor", ex);
                }
                throw;
            }
        }

        public TaskActions Tasks { get; private set; }

        public FileActions Files { get; private set; }

        public SynchronizationActions Synchronizations { get; private set; }

        public ResourceTimerManager TimerManager { get; private set; }

        internal FileSynchronizationLock FileLock { get; private set; }

        public void Initialize()
        {
            try
            {
                var generator = new UuidGenerator();
                storage.Initialize(generator, FileCodecs, storagePath =>
                {
                    if (configuration.RunInMemory)
                        return;

                    var resourceTypeFile = Path.Combine(storagePath, Constants.FileSystem.FsResourceMarker);

                    if (File.Exists(resourceTypeFile) == false)
                        using (File.Create(resourceTypeFile)) { }
                });
                generator.EtagBase = new SequenceActions(storage).GetNextValue("Raven/Etag");

                historian = new Historian(storage, new SynchronizationHiLo(storage));

                InitializeTriggersExceptIndexCodecs();

                search.Initialize(this);

                SecondStageInitialization();

                Files.InitializeTimer();
                Tasks.InitializeTimer();

                synchronizationTask.Start();
            }
            catch (Exception e)
            {
                Log.ErrorException(string.Format("Could not create file system '{0}'", Name ?? "unknown name"), e);
                try
                {
                    Dispose();
                }
                catch (Exception ex)
                {
                    Log.FatalException("Failed to dispose when already getting an error during file system initialization", ex);
                }
                throw;
            }
        }

        public static bool IsRemoteDifferentialCompressionInstalled
        {
            get
            {
                if (EnvironmentUtils.RunningOnPosix)
                    return false;
                try
                {
                    var rdcLibrary = new RdcLibrary();
                    Marshal.ReleaseComObject(rdcLibrary);

                    return true;
                }
                catch (COMException)
                {
                    return false;
                }
            }
        }

        private void ValidateStorage()
        {
            var storageEngineTypeName = configuration.FileSystem.SelectFileSystemStorageEngineAndFetchTypeName();
            if (InMemoryRavenConfiguration.VoronTypeName == storageEngineTypeName
                && configuration.Storage.Voron.AllowOn32Bits == false &&
                Environment.Is64BitProcess == false)
            {
                throw new Exception("Voron is prone to failure in 32-bits mode. Use " + Constants.Voron.AllowOn32Bits + " to force voron in 32-bit process.");
            }

            if (string.IsNullOrEmpty(configuration.FileSystem.DefaultStorageTypeName) == false &&
                configuration.FileSystem.DefaultStorageTypeName.Equals(storageEngineTypeName, StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new Exception(string.Format("The file system is configured to use '{0}' storage engine, but it points to '{1}' data", configuration.FileSystem.DefaultStorageTypeName, storageEngineTypeName));
            }

            if (configuration.RunInMemory == false && string.IsNullOrEmpty(configuration.FileSystem.DataDirectory) == false && Directory.Exists(configuration.FileSystem.DataDirectory))
            {
                var resourceTypeFiles = Directory.EnumerateFiles(configuration.FileSystem.DataDirectory, Constants.ResourceMarkerPrefix + "*").Select(Path.GetFileName).ToArray();
                
                if (resourceTypeFiles.Length == 0)
                    return;

                if (resourceTypeFiles.Length > 1)
                {
                    throw new Exception(string.Format("The file system directory cannot contain more than one resource file marker, but it contains: {0}", string.Join(", ", resourceTypeFiles)));
                }

                var resourceType = resourceTypeFiles[0];

                if (resourceType.Equals(Constants.FileSystem.FsResourceMarker) == false)
                {
                    throw new Exception(string.Format("The file system data directory contains data of a different resource kind: {0}", resourceType.Substring(Constants.ResourceMarkerPrefix.Length)));
                }
            }
        }

        public static ITransactionalStorage CreateTransactionalStorage(InMemoryRavenConfiguration configuration)
        {
            var storageType = configuration.FileSystem.SelectFileSystemStorageEngineAndFetchTypeName();

            switch (storageType)
            {
                case InMemoryRavenConfiguration.VoronTypeName:
                    return new TransactionalStorage(configuration);
                case InMemoryRavenConfiguration.EsentTypeName:
                    return new Storage.Esent.TransactionalStorage(configuration);
                
                default: 
                    throw new NotSupportedException("Unknown storage type name: " + storageType);
            }
        }

        public IDisposable DisableAllTriggersForCurrentThread(HashSet<Type> except = null)
        {
            if (disposed)
                return new DisposableAction(() => { });

            var old = disableAllTriggers.Value;
            disableAllTriggers.Value = new DisableTriggerState{Disabled = true, Except = except};
            return new DisposableAction(() =>
            {
                if (disposed)
                    return;

                try
                {
                    disableAllTriggers.Value = old;
                }
                catch (ObjectDisposedException)
                {
                }
            });
        }

        private void InitializeTriggersExceptIndexCodecs()
        {
            FileCodecs.OfType<IRequiresFileSystemInitialization>().Apply(initialization => initialization.Initialize(this));

            PutTriggers.Init(disableAllTriggers).OfType<IRequiresFileSystemInitialization>().Apply(initialization => initialization.Initialize(this));

            MetadataUpdateTriggers.Init(disableAllTriggers).OfType<IRequiresFileSystemInitialization>().Apply(initialization => initialization.Initialize(this));

            RenameTriggers.Init(disableAllTriggers).OfType<IRequiresFileSystemInitialization>().Apply(initialization => initialization.Initialize(this));

            DeleteTriggers.Init(disableAllTriggers).OfType<IRequiresFileSystemInitialization>().Apply(initialization => initialization.Initialize(this));

            ReadTriggers.Init(disableAllTriggers).OfType<IRequiresFileSystemInitialization>().Apply(initialization => initialization.Initialize(this));

            SynchronizationTriggers.Init(disableAllTriggers).OfType<IRequiresFileSystemInitialization>().Apply(initialization => initialization.Initialize(this));
        }

        private void SecondStageInitialization()
        {
            FileCodecs
                .OfType<IRequiresFileSystemInitialization>()
                .Apply(initialization => initialization.SecondStageInit());

            PutTriggers.Apply(initialization => initialization.SecondStageInit());

            MetadataUpdateTriggers.Apply(initialization => initialization.SecondStageInit());

            RenameTriggers.Apply(initialization => initialization.SecondStageInit());

            DeleteTriggers.Apply(initialization => initialization.SecondStageInit());

            ReadTriggers.Apply(initialization => initialization.SecondStageInit());

            SynchronizationTriggers.Apply(initialization => initialization.SecondStageInit());
        }

        /// <summary>
        ///     This is used to hold state associated with this instance by external extensions
        /// </summary>
        public AtomicDictionary<object> ExtensionsState { get; private set; }

        [ImportMany]
        public OrderedPartCollection<AbstractFileCodec> FileCodecs { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractFilePutTrigger> PutTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractFileMetadataUpdateTrigger> MetadataUpdateTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractFileRenameTrigger> RenameTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractFileDeleteTrigger> DeleteTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractFileReadTrigger> ReadTriggers { get; set; }

        [ImportMany]
        public OrderedPartCollection<AbstractSynchronizationTrigger> SynchronizationTriggers { get; set; }


        public ITransactionalStorage Storage
        {
            get { return storage; }
        }

        public IndexStorage Search
        {
            get { return search; }
        }

        public BufferPool BufferPool { get; private set; }

        public InMemoryRavenConfiguration Configuration
        {
            get { return configuration; }
        }

        public SigGenerator SigGenerator
        {
            get { return sigGenerator; }
        }

        public NotificationPublisher Publisher
        {
            get { return notificationPublisher; }
        }

        public Historian Historian
        {
            get { return historian; }
        }

        public FileLockManager FileLockManager
        {
            get { return fileLockManager; }
        }

        public SynchronizationTask SynchronizationTask
        {
            get { return synchronizationTask; }
        }

        public ConflictArtifactManager ConflictArtifactManager
        {
            get { return conflictArtifactManager; }
        }

        public ConflictDetector ConflictDetector
        {
            get { return conflictDetector; }
        }

        public ConflictResolver ConflictResolver
        {
            get { return conflictResolver; }
        }

        [CLSCompliant(false)]
        public MetricsCountersManager MetricsCounters
        {
            get { return metricsCounters; }
        }

        public TransportState TransportState
        {
            get { return transportState; }
        }

        public void Dispose()
        {
            if (disposed)
                return;

            // give it 3 seconds to complete requests
            for (int i = 0; i < 30 && Interlocked.Read(ref metricsCounters.ConcurrentRequestsCount) > 0; i++)
            {
                Thread.Sleep(100);
            }

            AppDomain.CurrentDomain.ProcessExit -= ShouldDispose;
            AppDomain.CurrentDomain.DomainUnload -= ShouldDispose;

            disposed = true;

            var exceptionAggregator = new ExceptionAggregator(Log, "Could not properly dispose RavenFileSystem");

            if (synchronizationTask != null)
                exceptionAggregator.Execute(synchronizationTask.Dispose);

            if (storage != null)
                exceptionAggregator.Execute(storage.Dispose);

            if (search != null)
                exceptionAggregator.Execute(search.Dispose);

            if (sigGenerator != null)
                exceptionAggregator.Execute(sigGenerator.Dispose);

            if (BufferPool != null)
                exceptionAggregator.Execute(BufferPool.Dispose);

            if (metricsCounters != null)
                exceptionAggregator.Execute(metricsCounters.Dispose);

            if (Tasks != null)
                Tasks.Dispose(exceptionAggregator);

            if (TimerManager != null)
                exceptionAggregator.Execute(TimerManager.Dispose);

            if (Files != null)
                exceptionAggregator.Execute(Files.Dispose);

            exceptionAggregator.Execute(disableAllTriggers.Dispose);

            exceptionAggregator.ThrowIfNeeded();
        }

        public FileSystemMetrics CreateMetrics()
        {
            var metrics = metricsCounters;
            
            return new FileSystemMetrics
            {
                RequestsPerSecond = Math.Round(metrics.RequestsPerSecondCounter.CurrentValue, 3),
                FilesWritesPerSecond = Math.Round(metrics.FilesPerSecond.CurrentValue, 3),

                RequestsDuration = metrics.RequestDurationMetric.CreateHistogramData(),
                Requests = metrics.ConcurrentRequests.CreateMeterData()
            };
        }

        private void ShouldDispose(object sender, EventArgs eventArgs)
        {
            Dispose();
        }

        public FileSystemStats GetFileSystemStats()
        {
            var fsStats = new FileSystemStats
            {
                Name = Name,
                FileSystemId = Storage.Id,
                Metrics = CreateMetrics(),
                ActiveSyncs = SynchronizationTask.Queue.Active.ToList(),
                PendingSyncs = SynchronizationTask.Queue.Pending.ToList(),
            };
            Storage.Batch(accessor =>
            {
                fsStats.FileCount = accessor.GetFileCount();
                fsStats.LastFileEtag = accessor.GetLastEtag();
            });
            return fsStats;
        }

        public class FileSynchronizationLock
        {
            private readonly SynchronizationTask task;

            private readonly PutSerialLock fileLock;

            public FileSynchronizationLock(SynchronizationTask task, InMemoryRavenConfiguration config)
            {
                this.task = task;
                fileLock = new PutSerialLock(config);
            }

            public IDisposable Lock()
            {
                return task.HasActiveDestinations ? fileLock.Lock() : null;
            }
        }
    }
}
