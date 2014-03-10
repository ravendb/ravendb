using System;
using System.Collections.Specialized;

using Raven.Abstractions.Util.Streams;
using Raven.Database.Config;
using Raven.Database.Server.Connections;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Infrastructure;
using Raven.Database.Server.RavenFS.Notifications;
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

		public RavenFileSystem(InMemoryRavenConfiguration systemConfiguration, TransportState transportState)
		{
			this.systemConfiguration = systemConfiguration;

            storage = CreateTransactionalStorage(systemConfiguration.FileSystemDataDirectory, systemConfiguration.Settings);
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

			AppDomain.CurrentDomain.ProcessExit += ShouldDispose;
			AppDomain.CurrentDomain.DomainUnload += ShouldDispose;
		}

        private static ITransactionalStorage CreateTransactionalStorage(string path, NameValueCollection settings)
        {
            var storageType = settings["Raven/FileSystem/StorageType"];
            switch (storageType)
            {
                case "voron":
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