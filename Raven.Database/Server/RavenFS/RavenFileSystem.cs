using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.RavenFS;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Config;
using Raven.Database.Server.Connections;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Infrastructure;
using Raven.Database.Server.RavenFS.Notifications;
using Raven.Database.Util;
using Raven.Database.Server.RavenFS.Search;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Synchronization;
using Raven.Database.Server.RavenFS.Synchronization.Conflictuality;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;

namespace Raven.Database.Server.RavenFS
{
	public class RavenFileSystem : IDisposable
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

		public RavenFileSystem(InMemoryRavenConfiguration systemConfiguration, TransportState transportState)
		{
			this.systemConfiguration = systemConfiguration;

		    var storageType = systemConfiguration.DefaultFileSystemStorageTypeName;
            if (string.Equals(InMemoryRavenConfiguration.VoronTypeName, storageType, StringComparison.OrdinalIgnoreCase) == false)
            {
                if (Directory.Exists(systemConfiguration.FileSystemDataDirectory) &&
                        Directory.EnumerateFileSystemEntries(systemConfiguration.FileSystemDataDirectory).Any())
                    throw new InvalidOperationException(
                        string.Format(
                            "We do not allow to run on a storage engine other then Voron, while we are in the early pre-release phase of RavenDB 3.0. You are currently running on {0}",
                            storageType));

                Trace.WriteLine("Forcing filesystem to run on Voron - pre release behavior only, mind " + Path.GetFileName(Path.GetDirectoryName(systemConfiguration.FileSystemDataDirectory)));
                storageType = InMemoryRavenConfiguration.VoronTypeName;
            }

            storage = CreateTransactionalStorage(storageType, systemConfiguration.FileSystemDataDirectory, systemConfiguration.Settings);
			search = new IndexStorage(systemConfiguration.FileSystemIndexStoragePath, systemConfiguration.Settings);
			sigGenerator = new SigGenerator();
			var replicationHiLo = new SynchronizationHiLo(storage);
			var sequenceActions = new SequenceActions(storage);
			this.transportState = transportState;
			notificationPublisher = new NotificationPublisher(transportState);
			fileLockManager = new FileLockManager();
			storage.Initialize();
			search.Initialize();
			var uuidGenerator = new UuidGenerator(sequenceActions);
			historian = new Historian(storage, replicationHiLo, uuidGenerator);
			BufferPool = new BufferPool(1024 * 1024 * 1024, 65 * 1024);
			conflictArtifactManager = new ConflictArtifactManager(storage, search);
			conflictDetector = new ConflictDetector();
			conflictResolver = new ConflictResolver();
			synchronizationTask = new SynchronizationTask(storage, sigGenerator, notificationPublisher, systemConfiguration);
			storageOperationsTask = new StorageOperationsTask(storage, search, notificationPublisher);
            metricsCounters = new MetricsCountersManager();

			AppDomain.CurrentDomain.ProcessExit += ShouldDispose;
			AppDomain.CurrentDomain.DomainUnload += ShouldDispose;
		}

        private static ITransactionalStorage CreateTransactionalStorage(string storageType, string path, NameValueCollection settings)
        {
            switch (storageType)
            {
                case InMemoryRavenConfiguration.VoronTypeName:
                    return new Storage.Voron.TransactionalStorage(path, settings);
                default:
                    return new Storage.Esent.TransactionalStorage(path, settings);
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

			storage.Dispose();
			search.Dispose();
			sigGenerator.Dispose();
			BufferPool.Dispose();
            metricsCounters.Dispose();
		}

        public FileSystemMetrics CreateMetrics()
        {
            var metrics = metricsCounters;

            var percentiles = metrics.RequestDuationMetric.Percentiles(0.5, 0.75, 0.95, 0.99, 0.999, 0.9999);

            return new FileSystemMetrics
            {
                RequestsPerSecond = Math.Round(metrics.RequestsPerSecondCounter.CurrentValue, 3),
                FilesWritesPerSecond = Math.Round(metrics.FilesPerSecond.CurrentValue, 3),

                RequestsDuration = new HistogramData
                {
                    Counter = metrics.RequestDuationMetric.Count,
                    Max = metrics.RequestDuationMetric.Max,
                    Mean = metrics.RequestDuationMetric.Mean,
                    Min = metrics.RequestDuationMetric.Min,
                    Stdev = metrics.RequestDuationMetric.StdDev,
                    Percentiles = new Dictionary<string, double>
                            {
                                {"50%", percentiles[0]},
                                {"75%", percentiles[1]},
                                {"95%", percentiles[2]},
                                {"99%", percentiles[3]},
                                {"99.9%", percentiles[4]},
                                {"99.99%", percentiles[5]},
                            }
                },
                Requests = new MeterData
                {
                    Count = metrics.ConcurrentRequests.Count,
                    FifteenMinuteRate = Math.Round(metrics.ConcurrentRequests.FifteenMinuteRate, 3),
                    FiveMinuteRate = Math.Round(metrics.ConcurrentRequests.FiveMinuteRate, 3),
                    MeanRate = Math.Round(metrics.ConcurrentRequests.MeanRate, 3),
                    OneMinuteRate = Math.Round(metrics.ConcurrentRequests.OneMinuteRate, 3),
                }
            };
        }

		private void ShouldDispose(object sender, EventArgs eventArgs)
		{
			Dispose();
		}

		//[MethodImpl(MethodImplOptions.Synchronized)]
		//public void Start(InMemoryRavenConfiguration config)
		//{
		//	config.DependencyResolver = new DelegateDependencyResolver(type =>
		//	{
		//		if (type == typeof(RavenFileSystem))
		//			return this;
		//		return null;
		//	}, type =>
		//	{
		//		if (type == typeof(RavenFileSystem))
		//			return new[] { this };
		//		return Enumerable.Empty<object>();
		//	});

		//	//TODO: check
		//	//config.Services.Replace(typeof(IHostBufferPolicySelector), new NoBufferPolicySelector());

		//	config.MessageHandlers.Add(new CachePreventingHandler());

		//	// we don't like XML, let us remove support for it.
		//	config.Formatters.XmlFormatter.SupportedMediaTypes.Clear();

		//	config.Formatters.JsonFormatter.SerializerSettings.Converters.Add(new IsoDateTimeConverter());
		//	// the default json parser can't handle NameValueCollection
		//	config.Formatters.JsonFormatter.SerializerSettings.Converters.Add(new NameValueCollectionJsonConverter());

		//	config.Routes.MapHttpRoute(
		//		name: "ClientAccessPolicy.xml",
		//		routeTemplate: "ravenfs/ClientAccessPolicy.xml",
		//		defaults: new { controller = "static", action = "ClientAccessPolicy" });

		//	config.Routes.MapHttpRoute(
		//		name: "favicon.ico",
		//		routeTemplate: "ravenfs/favicon.ico",
		//		defaults: new { controller = "static", action = "FavIcon" });

		//	config.Routes.MapHttpRoute(
		//		name: "RavenFS.Studio.xap",
		//		routeTemplate: "ravenfs/RavenFS.Studio.xap",
		//		defaults: new { controller = "static", action = "RavenStudioXap" });

		//	config.Routes.MapHttpRoute(
		//		name: "Id",
		//		routeTemplate: "ravenfs/id",
		//		defaults: new { controller = "static", action = "Id" });

		//	config.Routes.MapHttpRoute(
		//		name: "Empty",
		//		routeTemplate: "ravenfs/",
		//		defaults: new { controller = "static", action = "Root" });


		//	config.Routes.MapHttpRoute(
		//		name: "rdc",
		//		routeTemplate: "ravenfs/rdc/{action}/{*filename}",
		//		defaults: new { controller = "rdc", filename = RouteParameter.Optional }
		//		);

		//	config.Routes.MapHttpRoute(
		//		name: "synchronization",
		//		routeTemplate: "ravenfs/synchronization/{action}/{*filename}",
		//		defaults: new { controller = "synchronization", filename = RouteParameter.Optional }
		//		);

		//	config.Routes.MapHttpRoute(
		//		name: "folders",
		//		routeTemplate: "ravenfs/folders/{action}/{*directory}",
		//		defaults: new { controller = "folders", directory = RouteParameter.Optional }
		//		);

		//	config.Routes.MapHttpRoute(
		//		name: "search",
		//		routeTemplate: "ravenfs/search/{action}",
		//		defaults: new { controller = "search", action = "get" }
		//		);

		//	config.Routes.MapHttpRoute(
		//		name: "logs",
		//		routeTemplate: "ravenfs/search/{action}/{*type}",
		//		defaults: new { controller = "logs", action = "get", type = RouteParameter.Optional }
		//		);

		//	config.Routes.MapHttpRoute(
		//		name: "storage",
		//		routeTemplate: "ravenfs/storage/{action}/",
		//		defaults: new { controller = "storage" }
		//		);

		//	config.Routes.MapHttpRoute(
		//		name: "configsearch",
		//		routeTemplate: "ravenfs/config/search",
		//		defaults: new { controller = "config", action = "ConfigNamesStartingWith" }
		//		);

		//	config.Routes.MapHttpRoute(
		//		name: "Default",
		//		routeTemplate: "ravenfs/{controller}/{*name}",
		//		defaults: new { controller = "files", name = RouteParameter.Optional }
		//		);

		//	StorageOperationsTask.ResumeFileRenamingAsync();
		//}
	}
}