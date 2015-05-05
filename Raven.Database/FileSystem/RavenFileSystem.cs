using System;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Actions;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.Impl;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Connections;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Notifications;
using Raven.Database.Util;
using Raven.Database.FileSystem.Search;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.FileSystem.Synchronization.Conflictuality;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Raven.Abstractions.FileSystem;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper.Unmanaged;
using System.Runtime.InteropServices;
using Raven.Abstractions.Data;
using Raven.Database.FileSystem.Storage.Voron;
using TaskActions = Raven.Database.FileSystem.Actions.TaskActions;

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
		private readonly InMemoryRavenConfiguration systemConfiguration;
	    private readonly TransportState transportState;
        private readonly MetricsCountersManager metricsCounters;

		private readonly ThreadLocal<bool> disableAllTriggers = new ThreadLocal<bool>(() => false);

		private volatile bool disposed;

        private Historian historian;

        public string Name { get; private set; }

	    public string ResourceName { get; private set; }

	    public RavenFileSystem(InMemoryRavenConfiguration systemConfiguration, string name, TransportState receivedTransportState = null)
		{
			ExtensionsState = new AtomicDictionary<object>();

		    Name = name;
			ResourceName = string.Concat(Abstractions.Data.Constants.FileSystem.UrlPrefix, "/", name);
			this.systemConfiguration = systemConfiguration;

			systemConfiguration.Container.SatisfyImportsOnce(this);

            transportState = receivedTransportState ?? new TransportState();

            storage = CreateTransactionalStorage(systemConfiguration);

            sigGenerator = new SigGenerator();
            fileLockManager = new FileLockManager();			        
   
            BufferPool = new BufferPool(1024 * 1024 * 1024, 65 * 1024);
            conflictDetector = new ConflictDetector();
            conflictResolver = new ConflictResolver(storage, new CompositionContainer(systemConfiguration.Catalog));

            notificationPublisher = new NotificationPublisher(transportState);
            synchronizationTask = new SynchronizationTask(storage, sigGenerator, notificationPublisher, systemConfiguration);

            metricsCounters = new MetricsCountersManager();

            search = new IndexStorage(name, systemConfiguration);

            conflictArtifactManager = new ConflictArtifactManager(storage, search);

			Tasks = new TaskActions(this, Log);
			Files = new FileActions(this, Log);
			Synchronizations = new SynchronizationActions(this, Log);

			AppDomain.CurrentDomain.ProcessExit += ShouldDispose;
			AppDomain.CurrentDomain.DomainUnload += ShouldDispose;
		}

	    public TaskActions Tasks { get; private set; }

		public FileActions Files { get; private set; }

		public SynchronizationActions Synchronizations { get; private set; }

	    public void Initialize()
        {
		    var generator = new UuidGenerator();
		    storage.Initialize(generator, FileCodecs);
			generator.EtagBase = new SequenceActions(storage).GetNextValue("Raven/Etag");

            historian = new Historian(storage, new SynchronizationHiLo(storage));

			InitializeTriggersExceptIndexCodecs();

            search.Initialize(this);

			SecondStageInitialization();
        }

        public static bool IsRemoteDifferentialCompressionInstalled
        {
            get
            {
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

        internal static ITransactionalStorage CreateTransactionalStorage(InMemoryRavenConfiguration configuration)
        {
            // We select the most specific.
            var storageType = configuration.FileSystem.DefaultStorageTypeName;
            if (storageType == null) // We choose the system wide if not defined.
                storageType = configuration.DefaultStorageTypeName;

			if (storageType != null)
				storageType = storageType.ToLowerInvariant();

            switch (storageType)
            {
                case InMemoryRavenConfiguration.VoronTypeName:
					if (Environment.Is64BitProcess == false && configuration.Storage.Voron.AllowOn32Bits == false)
					{
						throw new Exception("Voron is prone to failure in 32-bits mode. Use " + Constants.Voron.AllowOn32Bits + " to force voron in 32-bit process.");
					}
                    return new Storage.Voron.TransactionalStorage(configuration);
                case InMemoryRavenConfiguration.EsentTypeName:
                    return new Storage.Esent.TransactionalStorage(configuration);
                
                default: // We choose esent by default.
                    return new Storage.Esent.TransactionalStorage(configuration);
            }
        }

		public IDisposable DisableAllTriggersForCurrentThread()
		{
			if (disposed)
				return new DisposableAction(() => { });

			bool old = disableAllTriggers.Value;
			disableAllTriggers.Value = true;
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
			get { return systemConfiguration; }
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
	            Metrics = CreateMetrics(),
	            ActiveSyncs = SynchronizationTask.Queue.Active.ToList(),
	            PendingSyncs = SynchronizationTask.Queue.Pending.ToList(),
	        };
	        Storage.Batch(accessor => { fsStats.FileCount = accessor.GetFileCount(); });
            return fsStats;
	    }

    }
}