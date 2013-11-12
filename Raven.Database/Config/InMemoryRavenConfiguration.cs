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
	using System.Runtime;

	using Raven.Abstractions.Util.Encryptors;

	public class InMemoryRavenConfiguration
	{
		private CompositionContainer container;
		private bool containerExternallySet;
		private string dataDirectory;
		private string pluginsDirectory;

		public InMemoryRavenConfiguration()
		{
			Settings = new NameValueCollection(StringComparer.OrdinalIgnoreCase);

			CreateAutoIndexesForAdHocQueriesIfNeeded = true;

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

			SetupGC();
		}

		public void Initialize()
		{
			int defaultMaxNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 128 * 1024 : 16 * 1024;
			int defaultInitialNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 512 : 256;

			var ravenSettings = new StronglyTypedRavenSettings(Settings);
			ravenSettings.Setup(defaultMaxNumberOfItemsToIndexInSingleBatch, defaultInitialNumberOfItemsToIndexInSingleBatch);

			// Core settings
			MaxPageSize = ravenSettings.MaxPageSize.Value;

			MemoryCacheLimitMegabytes = ravenSettings.MemoryCacheLimitMegabytes.Value;

			MemoryCacheExpiration = ravenSettings.MemoryCacheExpiration.Value;

			MemoryCacheLimitPercentage = ravenSettings.MemoryCacheLimitPercentage.Value;

			MemoryCacheLimitCheckInterval = ravenSettings.MemoryCacheLimitCheckInterval.Value;

			// Discovery
			DisableClusterDiscovery = ravenSettings.DisableClusterDiscovery.Value;

			// TODO: Validate the cluster name. Valid names are only valid chars in documents IDs.
			ClusterName = ravenSettings.ClusterName.Value;

			ServerName = ravenSettings.ServerName.Value;

			MaxStepsForScript = ravenSettings.MaxStepsForScript.Value;
			AdditionalStepsForScriptBasedOnDocumentSize = ravenSettings.AdditionalStepsForScriptBasedOnDocumentSize.Value;

			// Index settings
			MaxIndexingRunLatency = ravenSettings.MaxIndexingRunLatency.Value;
			MaxIndexWritesBeforeRecreate = ravenSettings.MaxIndexWritesBeforeRecreate.Value;
			MaxIndexOutputsPerDocument = ravenSettings.MaxIndexOutputsPerDocument.Value;

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
			InitialNumberOfItemsToReduceInSingleBatch = MaxNumberOfItemsToReduceInSingleBatch == ravenSettings.MaxNumberOfItemsToReduceInSingleBatch.Default ?
				 defaultInitialNumberOfItemsToIndexInSingleBatch / 2 :
				 Math.Max(16, Math.Min(MaxNumberOfItemsToIndexInSingleBatch / 256, defaultInitialNumberOfItemsToIndexInSingleBatch / 2));

			NumberOfItemsToExecuteReduceInSingleStep = ravenSettings.NumberOfItemsToExecuteReduceInSingleStep.Value;

			var initialNumberOfItemsToReduceInSingleBatch = Settings["Raven/InitialNumberOfItemsToReduceInSingleBatch"];
			if (initialNumberOfItemsToReduceInSingleBatch != null)
			{
				InitialNumberOfItemsToReduceInSingleBatch = Math.Min(int.Parse(initialNumberOfItemsToReduceInSingleBatch),
																	MaxNumberOfItemsToReduceInSingleBatch);
			}

			MaxNumberOfParallelIndexTasks = ravenSettings.MaxNumberOfParallelIndexTasks.Value;

			NewIndexInMemoryMaxBytes = ravenSettings.NewIndexInMemoryMaxMb.Value;

			MaxIndexCommitPointStoreTimeInterval = ravenSettings.MaxIndexCommitPointStoreTimeInterval.Value;

			MinIndexingTimeIntervalToStoreCommitPoint = ravenSettings.MinIndexingTimeIntervalToStoreCommitPoint.Value;

			MaxNumberOfStoredCommitPoints = ravenSettings.MaxNumberOfStoredCommitPoints.Value;

			// Data settings
			RunInMemory = ravenSettings.RunInMemory.Value;

			if (string.IsNullOrEmpty(DefaultStorageTypeName))
			{
				DefaultStorageTypeName = Settings["Raven/StorageTypeName"] ?? Settings["Raven/StorageEngine"] ?? "voron";
			}

			CreateAutoIndexesForAdHocQueriesIfNeeded = ravenSettings.CreateAutoIndexesForAdHocQueriesIfNeeded.Value;

			TimeToWaitBeforeRunningIdleIndexes = ravenSettings.TimeToWaitBeforeRunningIdleIndexes.Value;
			TimeToWaitBeforeMarkingAutoIndexAsIdle = ravenSettings.TimeToWaitBeforeMarkingAutoIndexAsIdle.Value;

			TimeToWaitBeforeMarkingIdleIndexAsAbandoned = ravenSettings.TimeToWaitBeforeMarkingIdleIndexAsAbandoned.Value;
			TimeToWaitBeforeRunningAbandonedIndexes = ravenSettings.TimeToWaitBeforeRunningAbandonedIndexes.Value;

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
			{
				Port = PortUtil.GetPort(ravenSettings.Port.Value);
				UseSsl = ravenSettings.UseSsl.Value;
			}

			SetVirtualDirectory();

			HttpCompression = ravenSettings.HttpCompression.Value;

			AccessControlAllowOrigin = ravenSettings.AccessControlAllowOrigin.Value;
			AccessControlMaxAge = ravenSettings.AccessControlMaxAge.Value;
			AccessControlAllowMethods = ravenSettings.AccessControlAllowMethods.Value;
			AccessControlRequestHeaders = ravenSettings.AccessControlRequestHeaders.Value;

			AnonymousUserAccessMode = GetAnonymousUserAccessMode();

			RedirectStudioUrl = ravenSettings.RedirectStudioUrl.Value;

			DisableDocumentPreFetchingForIndexing = ravenSettings.DisableDocumentPreFetchingForIndexing.Value;

			MaxNumberOfItemsToPreFetchForIndexing = ravenSettings.MaxNumberOfItemsToPreFetchForIndexing.Value;

			// Misc settings
			WebDir = ravenSettings.WebDir.Value;

			PluginsDirectory = ravenSettings.PluginsDirectory.Value.ToFullPath();

			CompiledIndexCacheDirectory = ravenSettings.CompiledIndexCacheDirectory.Value.ToFullPath();

			var taskSchedulerType = ravenSettings.TaskScheduler.Value;
			if (taskSchedulerType != null)
			{
				var type = Type.GetType(taskSchedulerType);
				CustomTaskScheduler = (TaskScheduler)Activator.CreateInstance(type);
			}

			AllowLocalAccessWithoutAuthorization = ravenSettings.AllowLocalAccessWithoutAuthorization.Value;

			PostInit();
		}

		public TimeSpan TimeToWaitBeforeRunningIdleIndexes { get; private set; }

		public TimeSpan TimeToWaitBeforeRunningAbandonedIndexes { get; private set; }

		public TimeSpan TimeToWaitBeforeMarkingAutoIndexAsIdle { get; private set; }

		public TimeSpan TimeToWaitBeforeMarkingIdleIndexAsAbandoned { get; private set; }

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

		public List<string> ActiveBundles
		{
			get
			{
				var activeBundles = Settings[Constants.ActiveBundles] ?? "";

				return activeBundles.GetSemicolonSeparatedValues();
			}
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

		private void SetupGC()
		{
			//GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
		}

		private static readonly Lazy<byte[]> defaultOauthKey = new Lazy<byte[]>(() =>
		{
			using (var rsa = Encryptor.Current.CreateAsymmetrical())
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
				return new UriBuilder(UseSsl ? "https" : "http", (HostName ?? Environment.MachineName), Port, VirtualDirectory).Uri.ToString();
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
		/// New indexes are kept in memory until they reach this integer value in bytes or until they're non-stale
		/// Default: 64 MB
		/// Minimum: 1 MB
		/// </summary>
		public int NewIndexInMemoryMaxBytes { get; set; }

		#endregion

		#region HTTP settings

		/// <summary>
		/// The hostname to use when creating the http listener (null to accept any hostname or address)
		/// Default: none, binds to all host names
		/// </summary>
		public string HostName { get; set; }

		/// <summary>
		/// Whatever we should use SSL for this connection
		/// </summary>
		public bool UseSsl { get; set; }

		/// <summary>
		/// Whatever we should use FIPS compliant encryption algorithms
		/// </summary>
		public bool UseFips { get; set; }

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
		public AnonymousUserAccessMode AnonymousUserAccessMode
		{
			get { return anonymousUserAccessMode; }
			set { anonymousUserAccessMode = value; }
		}

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
					.Concat(Catalog.Catalogs.OfType<Raven.Database.Plugins.Catalogs.FilteredCatalog>()
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

		/// <summary>
		/// Where to cache the compiled indexes
		/// Default: ~\Raven\CompiledIndexCache
		/// </summary>
		public string CompiledIndexCacheDirectory { get; set; }

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

		public int MaxNumberOfItemsToPreFetchForIndexing { get; set; }

		[JsonIgnore]
		public AggregateCatalog Catalog { get; set; }

		public bool RunInUnreliableYetFastModeThatIsNotSuitableForProduction { get; set; }

		private string indexStoragePath;
		private int? maxNumberOfParallelIndexTasks;
		private int initialNumberOfItemsToIndexInSingleBatch;
		private AnonymousUserAccessMode anonymousUserAccessMode;
		/// <summary>
		/// The expiration value for documents in the internal managed cache
		/// </summary>
		public TimeSpan MemoryCacheExpiration { get; set; }

		/// <summary>
		/// Controls whatever RavenDB will create temporary indexes 
		/// for queries that cannot be directed to standard indexes
		/// </summary>
		public bool CreateAutoIndexesForAdHocQueriesIfNeeded { get; set; }

		/// <summary>
		/// Maximum time interval for storing commit points for map indexes when new items were added.
		/// The commit points are used to restore index if unclean shutdown was detected.
		/// Default: 00:05:00 
		/// </summary>
		public TimeSpan MaxIndexCommitPointStoreTimeInterval { get; set; }

		/// <summary>
		/// Minumum interval between between successive indexing that will allow to store a  commit point
		/// Default: 00:01:00
		/// </summary>
		public TimeSpan MinIndexingTimeIntervalToStoreCommitPoint { get; set; }

		/// <summary>
		/// Maximum number of kept commit points to restore map index after unclean shutdown
		/// Default: 5
		/// </summary>
		public int MaxNumberOfStoredCommitPoints { get; set; }

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
		
		/// <summary>
		/// If True, cluster discovery will be disabled. Default is False
		/// </summary>
		public bool DisableClusterDiscovery { get; set; }

		/// <summary>
		/// The cluster name
		/// </summary>
		public string ClusterName { get; set; }

		/// <summary>
		/// The server name
		/// </summary>
		public string ServerName { get; set; }
		
		/// <summary>
		/// The maximum number of steps (instructions) to give a script before timing out.
		/// Default: 10,000
		/// </summary>
		public int MaxStepsForScript { get; set; }

		/// <summary>
		/// The number of additional steps to add to a given script based on the processed document's quota.
		/// Set to 0 to give use a fixed size quota. This value is multiplied with the doucment size.
		/// Default: 5
		/// </summary>
		public int AdditionalStepsForScriptBasedOnDocumentSize { get; set; }

		public int MaxIndexWritesBeforeRecreate { get; set; }

		/// <summary>
		/// Limits the number of map outputs that an index is allowed to create for a one source document. If a map operation applied to the one document
		/// produces more outputs than this number then an index definition will be considered as a suspicious and the index will be marked as errored.
		/// Default value: 15. In order to disable this check set value to -1.
		/// </summary>
		public int MaxIndexOutputsPerDocument { get; set; }

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
			return AnonymousUserAccessMode.Admin;
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
                case "voron":
                    storageEngine = typeof(Raven.Storage.Voron.TransactionalStorage).AssemblyQualifiedName;
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
				return typeof(Raven.Storage.Voron.TransactionalStorage).AssemblyQualifiedName;

			if (String.IsNullOrEmpty(DataDirectory) == false && Directory.Exists(DataDirectory))
			{
				if (File.Exists(Path.Combine(DataDirectory, "Raven.ravendb")))
				{
					return typeof(Raven.Storage.Managed.TransactionalStorage).AssemblyQualifiedName;
				}
                if (File.Exists(Path.Combine(DataDirectory, "Raven.voron")))
                {
                    return typeof(Raven.Storage.Voron.TransactionalStorage).AssemblyQualifiedName;
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
