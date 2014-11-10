using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using Raven.Abstractions.Data;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Connections;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Notifications;
using Raven.Database.Util;
using Raven.Database.FileSystem.Search;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.FileSystem.Synchronization.Conflictuality;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;
using Raven.Json.Linq;
using Raven.Abstractions.FileSystem;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper.Unmanaged;
using System.Runtime.InteropServices;

namespace Raven.Database.FileSystem
{
    public class RavenFileSystem : IResourceStore, IDisposable
	{
		private readonly ConflictArtifactManager conflictArtifactManager;
		private readonly ConflictDetector conflictDetector;
		private readonly ConflictResolver conflictResolver;
		private readonly FileLockManager fileLockManager;
		private readonly Historian historian;
		private readonly NotificationPublisher notificationPublisher;
		private readonly IndexStorage search;
		private readonly SigGenerator sigGenerator;
		private readonly ITransactionalStorage storage;
		private readonly StorageOperationsTask storageOperationsTask;
		private readonly SynchronizationTask synchronizationTask;
		private readonly InMemoryRavenConfiguration systemConfiguration;
	    private readonly TransportState transportState;
	    private readonly MetricsCountersManager metricsCounters;

        public string Name { get; private set; }

		public RavenFileSystem(InMemoryRavenConfiguration systemConfiguration, string name, TransportState recievedTransportState = null)
		{
		    this.Name = name;
			this.systemConfiguration = systemConfiguration;

            storage = CreateTransactionalStorage(systemConfiguration);
            search = new IndexStorage(name, systemConfiguration);

			sigGenerator = new SigGenerator();
			var replicationHiLo = new SynchronizationHiLo(storage);
			var sequenceActions = new SequenceActions(storage);
			transportState = recievedTransportState ?? new TransportState();
			notificationPublisher = new NotificationPublisher(transportState);
			fileLockManager = new FileLockManager();
			storage.Initialize();          

			var uuidGenerator = new UuidGenerator(sequenceActions);
			historian = new Historian(storage, replicationHiLo, uuidGenerator);
			BufferPool = new BufferPool(1024 * 1024 * 1024, 65 * 1024);
			conflictArtifactManager = new ConflictArtifactManager(storage, search);
			conflictDetector = new ConflictDetector();
			conflictResolver = new ConflictResolver(storage, new CompositionContainer(systemConfiguration.Catalog));
			synchronizationTask = new SynchronizationTask(storage, sigGenerator, notificationPublisher, systemConfiguration);
			storageOperationsTask = new StorageOperationsTask(storage, search, notificationPublisher);
            metricsCounters = new MetricsCountersManager();

			AppDomain.CurrentDomain.ProcessExit += ShouldDispose;
			AppDomain.CurrentDomain.DomainUnload += ShouldDispose;
		}

        public static bool IsRemoteDifferentialCompressionInstalled
        {
            get
            {
                try
                {
                    var _rdcLibrary = new RdcLibrary();
                    Marshal.ReleaseComObject(_rdcLibrary);

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
            var storageType = configuration.FileSystem.DefaultStorageTypeName;
            switch (storageType)
            {
                case InMemoryRavenConfiguration.VoronTypeName:
					return new Storage.Voron.TransactionalStorage(configuration);
                default:
					return new Storage.Esent.TransactionalStorage(configuration);
            }
        }

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

		public StorageOperationsTask StorageOperationsTask
		{
			get { return storageOperationsTask; }
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
			AppDomain.CurrentDomain.ProcessExit -= ShouldDispose;
			AppDomain.CurrentDomain.DomainUnload -= ShouldDispose;

			synchronizationTask.Dispose();
			storage.Dispose();
			search.Dispose();
			sigGenerator.Dispose();
			BufferPool.Dispose();
            metricsCounters.Dispose();
		}

        public FileSystemMetrics CreateMetrics()
        {
            var metrics = metricsCounters;
            
            return new FileSystemMetrics
            {
                RequestsPerSecond = Math.Round(metrics.RequestsPerSecondCounter.CurrentValue, 3),
                FilesWritesPerSecond = Math.Round(metrics.FilesPerSecond.CurrentValue, 3),

                RequestsDuration = metrics.RequestDuationMetric.CreateHistogramData(),
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