//-----------------------------------------------------------------------
// <copyright file="InMemoryRavenConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.ComponentModel.Composition.Hosting;
using System.ComponentModel.Composition.Primitives;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using System.Web;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Database.Plugins.Catalogs;
using Raven.Database.Server;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Config
{
	public class InMemoryRavenConfiguration
	{
		private CompositionContainer container;
		private bool containerExternallySet;
		private string dataDirectory;
		private string pluginsDirectory;

		public InMemoryRavenConfiguration()
		{
			Settings = new NameValueCollection(StringComparer.InvariantCultureIgnoreCase);

			CreateTemporaryIndexesForAdHocQueriesIfNeeded = true;

			CreatePluginsDirectoryIfNotExisting = true;
			CreateAnalyzersDirectoryIfNotExisting = true;


			IndexingScheduler = new FairIndexingSchedulerWithNewIndexesBias();

			Catalog = new AggregateCatalog(
				new AssemblyCatalog(typeof(DocumentDatabase).Assembly)
				);

			Catalog.Changed += (sender, args) => ResetContainer();
		}

		public string DatabaseName { get; set; }

		public void PostInit()
		{
			FilterActiveBundles();

			SetupOAuth();
		}

		public void Initialize()
		{
			int defaultMaxNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 128 * 1024 : 64 * 1024;
			int defaultInitialNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 512 : 256;

			var ravenSettings = new StronglyTypedRavenSettings(Settings);
			ravenSettings.Setup(defaultMaxNumberOfItemsToIndexInSingleBatch, defaultInitialNumberOfItemsToIndexInSingleBatch);

			// Core settings
			MaxPageSize = ravenSettings.MaxPageSize.Value;

			MemoryCacheLimitMegabytes = ravenSettings.MemoryCacheLimitMegabytes.Value;

			MemoryCacheExpiration = ravenSettings.MemoryCacheExpiration.Value;

			MemoryCacheLimitPercentage = ravenSettings.MemoryCacheLimitPercentage.Value;

			MemoryCacheLimitCheckInterval = ravenSettings.MemoryCacheLimitCheckInterval.Value;

			// Index settings
			MaxIndexingRunLatency = ravenSettings.MaxIndexingRunLatency.Value;

			MaxNumberOfItemsToIndexInSingleBatch = ravenSettings.MaxNumberOfItemsToIndexInSingleBatch.Value;

			var initialNumberOfItemsToIndexInSingleBatch = Settings["Raven/InitialNumberOfItemsToIndexInSingleBatch"];
			if (initialNumberOfItemsToIndexInSingleBatch != null)
			{
				InitialNumberOfItemsToIndexInSingleBatch = Math.Min(int.Parse(initialNumberOfItemsToIndexInSingleBatch),
				                                                    MaxNumberOfItemsToIndexInSingleBatch);
			}
			else
			{
				InitialNumberOfItemsToIndexInSingleBatch = MaxNumberOfItemsToIndexInSingleBatch == ravenSettings.MaxNumberOfItemsToIndexInSingleBatch.Default ?
				 defaultInitialNumberOfItemsToIndexInSingleBatch :
				 Math.Max(16, Math.Min(MaxNumberOfItemsToIndexInSingleBatch / 256, defaultInitialNumberOfItemsToIndexInSingleBatch));
			}
			AvailableMemoryForRaisingIndexBatchSizeLimit = ravenSettings.AvailableMemoryForRaisingIndexBatchSizeLimit.Value;

		

			MaxNumberOfItemsToReduceInSingleBatch = ravenSettings.MaxNumberOfItemsToReduceInSingleBatch.Value;
			InitialNumberOfItemsToReduceInSingleBatch = MaxNumberOfItemsToReduceInSingleBatch == ravenSettings.MaxNumberOfItemsToReduceInSingleBatch.Default?
				 defaultInitialNumberOfItemsToIndexInSingleBatch/2 :
				 Math.Max(16, Math.Min(MaxNumberOfItemsToIndexInSingleBatch / 256, defaultInitialNumberOfItemsToIndexInSingleBatch / 2));

			NumberOfItemsToExecuteReduceInSingleStep = ravenSettings.NumberOfItemsToExecuteReduceInSingleStep.Value;

			var initialNumberOfItemsToReduceInSingleBatch = Settings["Raven/InitialNumberOfItemsToReduceInSingleBatch"];
			if (initialNumberOfItemsToReduceInSingleBatch != null)
			{
				InitialNumberOfItemsToReduceInSingleBatch = Math.Min(int.Parse(initialNumberOfItemsToReduceInSingleBatch),
																	MaxNumberOfItemsToReduceInSingleBatch);
			}

			MaxNumberOfParallelIndexTasks = ravenSettings.MaxNumberOfParallelIndexTasks.Value;

			TempIndexPromotionMinimumQueryCount = ravenSettings.TempIndexPromotionMinimumQueryCount.Value;

			TempIndexPromotionThreshold = ravenSettings.TempIndexPromotionThreshold.Value;

			TempIndexCleanupPeriod = ravenSettings.TempIndexCleanupPeriod.Value;

			TempIndexCleanupThreshold = ravenSettings.TempIndexCleanupThreshold.Value;

			TempIndexInMemoryMaxBytes = ravenSettings.TempIndexInMemoryMaxMb.Value;

			// Data settings
			RunInMemory = ravenSettings.RunInMemory.Value;

			if (string.IsNullOrEmpty(DefaultStorageTypeName))
			{
				DefaultStorageTypeName = Settings["Raven/StorageTypeName"] ?? Settings["Raven/StorageEngine"] ?? "esent";
			}

			CreateTemporaryIndexesForAdHocQueriesIfNeeded = ravenSettings.CreateTemporaryIndexesForAdHocQueriesIfNeeded.Value;

			ResetIndexOnUncleanShutdown = ravenSettings.ResetIndexOnUncleanShutdown.Value;

			SetupTransactionMode();

			DataDirectory = ravenSettings.DataDir.Value;

			var indexStoragePathSettingValue = ravenSettings.IndexStoragePath.Value;
			if (string.IsNullOrEmpty(indexStoragePathSettingValue) == false)
			{
				IndexStoragePath = indexStoragePathSettingValue;
			}

			// HTTP settings
			HostName = ravenSettings.HostName.Value;

			if (string.IsNullOrEmpty(DatabaseName)) // we only use this for root database
				Port = PortUtil.GetPort(ravenSettings.Port.Value);
			SetVirtualDirectory();

			HttpCompression = ravenSettings.HttpCompression.Value;

			AccessControlAllowOrigin = ravenSettings.AccessControlAllowOrigin.Value;
			AccessControlMaxAge = ravenSettings.AccessControlMaxAge.Value;
			AccessControlAllowMethods = ravenSettings.AccessControlAllowMethods.Value;
			AccessControlRequestHeaders = ravenSettings.AccessControlRequestHeaders.Value;

			AnonymousUserAccessMode = GetAnonymousUserAccessMode();

			RedirectStudioUrl = ravenSettings.RedirectStudioUrl.Value;

			DisableDocumentPreFetchingForIndexing = ravenSettings.DisableDocumentPreFetchingForIndexing.Value;

			// Misc settings
			WebDir = ravenSettings.WebDir.Value;

			PluginsDirectory = ravenSettings.PluginsDirectory.Value.ToFullPath();

			var taskSchedulerType = ravenSettings.TaskScheduler.Value;
			if (taskSchedulerType != null)
			{
				var type = Type.GetType(taskSchedulerType);
				CustomTaskScheduler = (TaskScheduler)Activator.CreateInstance(type);
			}

			AllowLocalAccessWithoutAuthorization = ravenSettings.AllowLocalAccessWithoutAuthorization.Value;

			PostInit();
		}

		private void FilterActiveBundles()
		{
			var activeBundles = Settings["Raven/ActiveBundles"] ?? "";

			var bundles = activeBundles.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
				.Select(x => x.Trim())
				.ToArray();

			if (container != null)
				container.Dispose();
			container = null;

			var catalog = GetUnfilteredCatalogs(Catalog.Catalogs);

			Catalog.Catalogs.Clear();

			Catalog.Catalogs.Add(new BundlesFilteredCatalog(catalog, bundles));

			var exportedValues = Container.GetExportedValues<IStartupTask>().ToArray();
		}

		private ComposablePartCatalog GetUnfilteredCatalogs(ICollection<ComposablePartCatalog> catalogs)
		{
			if (catalogs.Count != 1) 
				return new AggregateCatalog(catalogs.Select(GetUnfilteredCatalog));
			return GetUnfilteredCatalog(catalogs.First());
		}

		private static ComposablePartCatalog GetUnfilteredCatalog(ComposablePartCatalog x)
		{
			var filteredCatalog = x as BundlesFilteredCatalog;
			if (filteredCatalog != null)
				return GetUnfilteredCatalog(filteredCatalog.CatalogToFilter);
			return x;
		}

		public TaskScheduler CustomTaskScheduler { get; set; }

		public string RedirectStudioUrl { get; set; }

		private void SetupTransactionMode()
		{
			var transactionMode = Settings["Raven/TransactionMode"];
			TransactionMode result;
			if (Enum.TryParse(transactionMode, true, out result) == false)
				result = TransactionMode.Safe;
			TransactionMode = result;
		}

		private void SetVirtualDirectory()
		{
			var defaultVirtualDirectory = "/";
			try
			{
				if (HttpContext.Current != null)
					defaultVirtualDirectory = HttpContext.Current.Request.ApplicationPath;
			}
			catch (HttpException)
			{
				// explicitly ignoring this because we might be running in embedded mode
				// inside IIS during init stages, in which case we can't access the HttpContext
				// nor do we actually care
			}

			VirtualDirectory = Settings["Raven/VirtualDirectory"] ?? defaultVirtualDirectory;

		}

		public bool UseDefaultOAuthTokenServer
		{
			get { return Settings["Raven/OAuthTokenServer"] == null;  }
		}

		private void SetupOAuth()
		{
			OAuthTokenServer = Settings["Raven/OAuthTokenServer"] ??
							   (ServerUrl.EndsWith("/") ? ServerUrl + "OAuth/API-Key" : ServerUrl + "/OAuth/API-Key");
			OAuthTokenKey = GetOAuthKey();
		}

		private static readonly Lazy<byte[]> defaultOauthKey = new Lazy<byte[]>(() =>
		{
			using (var rsa = new RSACryptoServiceProvider())
			{
				return rsa.ExportCspBlob(true);
			}
		});

		private byte[] GetOAuthKey()
		{
			var key = Settings["Raven/OAuthTokenCertificate"];
			if (string.IsNullOrEmpty(key) == false)
			{
				return Convert.FromBase64String(key);
			}
			return defaultOauthKey.Value; // ensure we only create this once per process
		}

		public NameValueCollection Settings { get; set; }

		public string ServerUrl
		{
			get
			{
				HttpRequest httpRequest = null;
				try
				{
					if (HttpContext.Current != null)
						httpRequest = HttpContext.Current.Request;
				}
				catch (Exception)
				{
					// the issue is probably Request is not available in this context
					// we can safely ignore this, at any rate
				}
				if (httpRequest != null)// running in IIS, let us figure out how
				{
					var url = httpRequest.Url;
					return new UriBuilder(url)
					{
						Path = httpRequest.ApplicationPath,
						Query = ""
					}.Uri.ToString();
				}
				return new UriBuilder("http", (HostName ?? Environment.MachineName), Port, VirtualDirectory).Uri.ToString();
			}
		}

		#region Core settings

		/// <summary>
		/// When the database is shut down rudely, determine whatever to reset the index or to check it.
		/// Checking the index may take some time on large databases
		/// </summary>
		public bool ResetIndexOnUncleanShutdown { get; set; }

		/// <summary>
		/// The maximum allowed page size for queries. 
		/// Default: 1024
		/// Minimum: 10
		/// </summary>
		public int MaxPageSize { get; set; }

		/// <summary>
		/// Percentage of physical memory used for caching
		/// Allowed values: 0-99 (0 = autosize)
		/// </summary>
		public int MemoryCacheLimitPercentage { get; set; }

		/// <summary>
		/// An integer value that specifies the maximum allowable size, in megabytes, that caching 
		/// document instances will use
		/// </summary>
		public int MemoryCacheLimitMegabytes { get; set; }

		/// <summary>
		/// Interval for checking the memory cache limits
		/// Allowed values: max precision is 1 second
		/// Default: 00:02:00 (or value provided by system.runtime.caching app config)
		/// </summary>
		public TimeSpan MemoryCacheLimitCheckInterval { get; set; }
		#endregion

		#region Index settings

		/// <summary>
		/// The indexing scheduler to use
		/// </summary>
		public IIndexingScheduler IndexingScheduler { get; set; }

		/// <summary>
		/// Max number of items to take for indexing in a batch
		/// Minimum: 128
		/// </summary>
		public int MaxNumberOfItemsToIndexInSingleBatch { get; set; }

		/// <summary>
		/// The initial number of items to take when indexing a batch
		/// Default: 512 or 256 depending on CPU architecture
		/// </summary>
		public int InitialNumberOfItemsToIndexInSingleBatch
		{
			get { return initialNumberOfItemsToIndexInSingleBatch; }
			set { initialNumberOfItemsToIndexInSingleBatch = value; }
		}

		/// <summary>
		/// Max number of items to take for reducing in a batch
		/// Minimum: 128
		/// </summary>
		public int MaxNumberOfItemsToReduceInSingleBatch { get; set; }

		/// <summary>
		/// The initial number of items to take when reducing a batch
		/// Default: 256 or 128 depending on CPU architecture
		/// </summary>
		public int InitialNumberOfItemsToReduceInSingleBatch { get; set; }

		/// <summary>
		/// The number that controls the if single step reduce optimization is performed.
		/// If the count of mapped results if less than this value then the reduce is executed in single step.
		/// Default: 1024
		/// </summary>
		public int NumberOfItemsToExecuteReduceInSingleStep { get; set; }

		/// <summary>
		/// The maximum number of indexing tasks allowed to run in parallel
		/// Default: The number of processors in the current machine
		/// </summary>
		public int MaxNumberOfParallelIndexTasks
		{
			get
			{
				if (MemoryStatistics.MaxParallelismSet)
					return Math.Min(maxNumberOfParallelIndexTasks ?? MemoryStatistics.MaxParallelism, MemoryStatistics.MaxParallelism);
				return maxNumberOfParallelIndexTasks ?? Environment.ProcessorCount;
			}
			set { maxNumberOfParallelIndexTasks = value; }
		}

		/// <summary>
		/// Time (in milliseconds) the index has to be queried at least once in order for it to
		/// become permanent
		/// Default: 60000 (once per minute)
		/// </summary>
		public int TempIndexPromotionThreshold { get; set; }

		/// <summary>
		/// How many times a temporary, auto-generated index has to be accessed before it can
		/// be promoted to be a permanent one
		/// Default: 100
		/// </summary>
		public int TempIndexPromotionMinimumQueryCount { get; set; }

		/// <summary>
		/// How often to run the temporary index cleanup process (in seconds)
		/// Default: 600 (10 minutes)
		/// </summary>
		public TimeSpan TempIndexCleanupPeriod { get; set; }

		/// <summary>
		/// How much time in seconds to wait after a temporary index has been used before removing it if no further
		/// calls were made to it during that time
		/// Default: 1200 (20 minutes)
		/// </summary>
		public TimeSpan TempIndexCleanupThreshold { get; set; }

		/// <summary>
		/// Temp indexes are kept in memory until they reach this integer value in bytes
		/// Default: 25 MB
		/// Minimum: 1 MB
		/// </summary>
		public int TempIndexInMemoryMaxBytes { get; set; }

		#endregion

		#region HTTP settings

		/// <summary>
		/// The hostname to use when creating the http listener (null to accept any hostname or address)
		/// Default: none, binds to all host names
		/// </summary>
		public string HostName { get; set; }

		/// <summary>
		/// The port to use when creating the http listener. 
		/// Default: 8080. You can set it to *, in which case it will find the first available port from 8080 and upward.
		/// </summary>
		public int Port { get; set; }

		/// <summary>
		/// Determine the value of the Access-Control-Allow-Origin header sent by the server. 
		/// Indicates the URL of a site trusted to make cross-domain requests to this server.
		/// Allowed values: null (don't send the header), *, http://example.org (space separated if multiple sites)
		/// </summary>
		public string AccessControlAllowOrigin { get; set; }

		/// <summary>
		/// Determine the value of the Access-Control-Max-Age header sent by the server.
		/// Indicates how long (seconds) the browser should cache the Access Control settings.
		/// Ignored if AccessControlAllowOrigin is not specified.
		/// Default: 1728000 (20 days)
		/// </summary>
		public string AccessControlMaxAge { get; set; }

		/// <summary>
		/// Determine the value of the Access-Control-Allow-Methods header sent by the server.
		/// Indicates which HTTP methods (verbs) are permitted for requests from allowed cross-domain origins.
		/// Ignored if AccessControlAllowOrigin is not specified.
		/// Default: PUT,PATCH,GET,DELETE,POST
		/// </summary>
		public string AccessControlAllowMethods { get; set; }

		/// <summary>
		/// Determine the value of the Access-Control-Request-Headers header sent by the server.
		/// Indicates which HTTP headers are permitted for requests from allowed cross-domain origins.
		/// Ignored if AccessControlAllowOrigin is not specified.
		/// Allowed values: null (allow whatever headers are being requested), HTTP header field name
		/// </summary>
		public string AccessControlRequestHeaders { get; set; }

		private string virtualDirectory;

		/// <summary>
		/// The virtual directory to use when creating the http listener. 
		/// Default: / 
		/// </summary>
		public string VirtualDirectory
		{
			get { return virtualDirectory; }
			set
			{
				virtualDirectory = value;

				if (virtualDirectory.EndsWith("/"))
					virtualDirectory = virtualDirectory.Substring(0, virtualDirectory.Length - 1);
				if (virtualDirectory.StartsWith("/") == false)
					virtualDirectory = "/" + virtualDirectory;
			}
		}

		/// <summary>
		/// Whether to use http compression or not. 
		/// Allowed values: true/false; 
		/// Default: true
		/// </summary>
		public bool HttpCompression { get; set; }

		/// <summary>
		/// Defines which operations are allowed for anonymous users.
		/// Allowed values: All, Get, None
		/// Default: Get
		/// </summary>
		public AnonymousUserAccessMode AnonymousUserAccessMode { get; set; }

		/// <summary>
		/// If set local request don't require authentication
		/// Allowed values: true/false
		/// Default: false
		/// </summary>
		public bool AllowLocalAccessWithoutAuthorization { get; set; }

		/// <summary>
		/// The certificate to use when verifying access token signatures for OAuth
		/// </summary>
		public byte[] OAuthTokenKey { get; set; }

		#endregion

		#region Data settings

		/// <summary>
		/// The directory for the RavenDB database. 
		/// You can use the ~\ prefix to refer to RavenDB's base directory. 
		/// Default: ~\Data
		/// </summary>
		public string DataDirectory
		{
			get { return dataDirectory; }
			set { dataDirectory = value == null ? null : value.ToFullPath(); }
		}

		/// <summary>
		/// What storage type to use (see: RavenDB Storage engines)
		/// Allowed values: esent, munin
		/// Default: esent
		/// </summary>
		public string DefaultStorageTypeName
		{
			get { return defaultStorageTypeName; }
			set { if (!string.IsNullOrEmpty(value)) defaultStorageTypeName = value; }
		}
		private string defaultStorageTypeName;

		private bool runInMemory;

		/// <summary>
		/// Should RavenDB's storage be in-memory. If set to true, Munin would be used as the
		/// storage engine, regardless of what was specified for StorageTypeName
		/// Allowed values: true/false
		/// Default: false
		/// </summary>
		public bool RunInMemory
		{
			get { return runInMemory; }
			set
			{
				runInMemory = value;
				Settings["Raven/RunInMemory"] = value.ToString();
			}
		}

		/// <summary>
		/// What sort of transaction mode to use. 
		/// Allowed values: 
		/// Lazy - faster, but can result in data loss in the case of server crash. 
		/// Safe - slower, but will never lose data 
		/// Default: Safe 
		/// </summary>
		public TransactionMode TransactionMode { get; set; }

		#endregion

		#region Misc settings

		/// <summary>
		/// The directory to search for RavenDB's WebUI. 
		/// This is usually only useful if you are debugging RavenDB's WebUI. 
		/// Default: ~/Raven/WebUI 
		/// </summary>
		public string WebDir { get; set; }

		/// <summary>
		/// Where to look for plugins for RavenDB. 
		/// Default: ~\Plugins
		/// </summary>
		public string PluginsDirectory
		{
			get { return pluginsDirectory; }
			set
			{
				ResetContainer();
				// remove old directory catalog
				var matchingCatalogs = Catalog.Catalogs.OfType<DirectoryCatalog>()
					.Concat(Catalog.Catalogs.OfType<FilteredCatalog>()
								.Select(x => x.CatalogToFilter as DirectoryCatalog)
								.Where(x => x != null)
					)
					.Where(c => c.Path == pluginsDirectory)
					.ToArray();
				foreach (var cat in matchingCatalogs)
				{
					Catalog.Catalogs.Remove(cat);
				}

				pluginsDirectory = value.ToFullPath();

				// add new one
				if (Directory.Exists(pluginsDirectory))
				{
					var patterns = Settings["Raven/BundlesSearchPattern"] ?? "*.dll";
					foreach (var pattern in patterns.Split(new[] { ";" }, StringSplitOptions.RemoveEmptyEntries))
					{
						Catalog.Catalogs.Add(new BuiltinFilteringCatalog(new DirectoryCatalog(pluginsDirectory, pattern)));
					}
				}
			}
		}

		public bool CreatePluginsDirectoryIfNotExisting { get; set; }
		public bool CreateAnalyzersDirectoryIfNotExisting { get; set; }

		public string OAuthTokenServer { get; set; }

		#endregion

		[JsonIgnore]
		public CompositionContainer Container
		{
			get { return container ?? (container = new CompositionContainer(Catalog)); }
			set
			{
				containerExternallySet = true;
				container = value;
			}
		}

		public bool DisableDocumentPreFetchingForIndexing { get; set; }

		[JsonIgnore]
		public AggregateCatalog Catalog { get; set; }

		public bool RunInUnreliableYetFastModeThatIsNotSuitableForProduction { get; set; }

		private string indexStoragePath;
		private int? maxNumberOfParallelIndexTasks;
		private int initialNumberOfItemsToIndexInSingleBatch;
		/// <summary>
		/// The expiration value for documents in the internal managed cache
		/// </summary>
		public TimeSpan MemoryCacheExpiration { get; set; }

		/// <summary>
		/// Controls whatever RavenDB will create temporary indexes 
		/// for queries that cannot be directed to standard indexes
		/// </summary>
		public bool CreateTemporaryIndexesForAdHocQueriesIfNeeded { get; set; }

		public string IndexStoragePath
		{
			get
			{
				if (string.IsNullOrEmpty(indexStoragePath))
					indexStoragePath = Path.Combine(DataDirectory, "Indexes");
				return indexStoragePath;
			}
			set { indexStoragePath = value.ToFullPath(); }
		}

		public int AvailableMemoryForRaisingIndexBatchSizeLimit { get; set; }

		public TimeSpan MaxIndexingRunLatency { get; set; }

		internal bool IsTenantDatabase { get; set; }

		[Browsable(false)]
		[EditorBrowsable(EditorBrowsableState.Never)]
		public void SetSystemDatabase()
		{
			IsTenantDatabase = false;
		}

		[Browsable(false)]
		[EditorBrowsable(EditorBrowsableState.Never)]
		public bool IsSystemDatabase()
		{
			return IsTenantDatabase == false;
		}

		protected void ResetContainer()
		{
			if (Container != null && containerExternallySet == false)
			{
				Container.Dispose();
				Container = null;
			}
		}

		protected AnonymousUserAccessMode GetAnonymousUserAccessMode()
		{
			if (string.IsNullOrEmpty(Settings["Raven/AnonymousAccess"]) == false)
			{
				var val = Enum.Parse(typeof(AnonymousUserAccessMode), Settings["Raven/AnonymousAccess"]);
				return (AnonymousUserAccessMode)val;
			}
			return AnonymousUserAccessMode.Get;
		}

		public string GetFullUrl(string baseUrl)
		{
			if (baseUrl.StartsWith("/"))
				baseUrl = baseUrl.Substring(1);
			if (VirtualDirectory.EndsWith("/"))
				return VirtualDirectory + baseUrl;
			return VirtualDirectory + "/" + baseUrl;
		}

		public T? GetConfigurationValue<T>(string configName) where T : struct
		{
			// explicitly fail if we can't convert it
			if (string.IsNullOrEmpty(Settings[configName]) == false)
				return (T)Convert.ChangeType(Settings[configName], typeof(T));
			return null;
		}

		public ITransactionalStorage CreateTransactionalStorage(Action notifyAboutWork)
		{
			var storageEngine = SelectStorageEngine();
			switch (storageEngine.ToLowerInvariant())
			{
				case "esent":
					storageEngine = typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;
					break;
				case "munin":
					storageEngine = typeof(Raven.Storage.Managed.TransactionalStorage).AssemblyQualifiedName;
					break;
			}
			var type = Type.GetType(storageEngine);

			if (type == null)
				throw new InvalidOperationException("Could not find transactional storage type: " + storageEngine);

			return (ITransactionalStorage)Activator.CreateInstance(type, this, notifyAboutWork);
		}

		private string SelectStorageEngine()
		{
			if (RunInMemory)
				return typeof(Raven.Storage.Managed.TransactionalStorage).AssemblyQualifiedName;

			if (String.IsNullOrEmpty(DataDirectory) == false && Directory.Exists(DataDirectory))
			{
				if (File.Exists(Path.Combine(DataDirectory, "Raven.ravendb")))
				{
					return typeof(Raven.Storage.Managed.TransactionalStorage).AssemblyQualifiedName;
				}
				if (File.Exists(Path.Combine(DataDirectory, "Data")))
					return typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;
			}
			return DefaultStorageTypeName;
		}

		public void Dispose()
		{
			if (containerExternallySet)
				return;
			if (container == null)
				return;

			container.Dispose();
			container = null;
		}

		private ExtensionsLog GetExtensionsFor(Type type)
		{
			var enumerable =
				Container.GetExports(new ImportDefinition(x => true, type.FullName, ImportCardinality.ZeroOrMore, false, false)).
					ToArray();
			if (enumerable.Length == 0)
				return null;
			return new ExtensionsLog
			{
				Name = type.Name,
				Installed = enumerable.Select(export => new ExtensionsLogDetail
				{
					Assembly = export.Value.GetType().Assembly.GetName().Name,
					Name = export.Value.GetType().Name
				}).ToArray()
			};
		}

		public IEnumerable<ExtensionsLog> ReportExtensions(params Type[] types)
		{
			return types.Select(GetExtensionsFor).Where(extensionsLog => extensionsLog != null);
		}

		public void CustomizeValuesForTenant(string tenantId)
		{
			if (string.IsNullOrEmpty(Settings["Raven/IndexStoragePath"]) == false)
				Settings["Raven/IndexStoragePath"] = Path.Combine(Settings["Raven/IndexStoragePath"], "Databases", tenantId);

			if (string.IsNullOrEmpty(Settings["Raven/Esent/LogsPath"]) == false)
				Settings["Raven/Esent/LogsPath"] = Path.Combine(Settings["Raven/Esent/LogsPath"], "Databases", tenantId);
		}

		public void CopyParentSettings(InMemoryRavenConfiguration defaultConfiguration)
		{
			Port = defaultConfiguration.Port;
			OAuthTokenKey = defaultConfiguration.OAuthTokenKey;
			OAuthTokenServer = defaultConfiguration.OAuthTokenServer;
		}

		public IEnumerable<string> GetConfigOptionsDocs()
		{
			return ConfigOptionDocs.OptionsDocs;
		}
	}
}
