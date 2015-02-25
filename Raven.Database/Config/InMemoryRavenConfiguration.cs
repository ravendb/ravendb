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
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Plugins.Catalogs;
using Raven.Database.Server;
using Raven.Database.FileSystem.Util;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Enum = System.Enum;

namespace Raven.Database.Config
{
	public class InMemoryRavenConfiguration
	{
	    public const string VoronTypeName = "voron";
	    public const string EsentTypeName = "esent";

		private CompositionContainer container;
		private bool containerExternallySet;
		private string dataDirectory;
		private string pluginsDirectory;

		public ReplicationConfiguration Replication { get; private set; }

		public PrefetcherConfiguration Prefetcher { get; private set; }

		public StorageConfiguration Storage { get; private set; }

        public FileSystemConfiguration FileSystem { get; private set; }

		public EncryptionConfiguration Encryption { get; private set; }

		public IndexingConfiguration Indexing { get; set; }

		public InMemoryRavenConfiguration()
		{
			Replication = new ReplicationConfiguration();
			Prefetcher = new PrefetcherConfiguration();
			Storage = new StorageConfiguration();
            FileSystem = new FileSystemConfiguration();
			Encryption = new EncryptionConfiguration();
			Indexing = new IndexingConfiguration();

			Settings = new NameValueCollection(StringComparer.OrdinalIgnoreCase);

			CreateAutoIndexesForAdHocQueriesIfNeeded = true;

			CreatePluginsDirectoryIfNotExisting = true;
			CreateAnalyzersDirectoryIfNotExisting = true;

			IndexingClassifier = new DefaultIndexingClassifier();

			Catalog = new AggregateCatalog(new AssemblyCatalog(typeof(DocumentDatabase).Assembly));

			Catalog.Changed += (sender, args) => ResetContainer();
		}

        public string DatabaseName { get; set; }

        public string FileSystemName { get; set; }

        public string CountersDatabaseName { get; set; }

		public void PostInit()
		{
			FilterActiveBundles();

			SetupOAuth();

			SetupGC();
		}

		public InMemoryRavenConfiguration Initialize()
		{
			int defaultMaxNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 128 * 1024 : 16 * 1024;
			int defaultInitialNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 512 : 256;

			var ravenSettings = new StronglyTypedRavenSettings(Settings);
			ravenSettings.Setup(defaultMaxNumberOfItemsToIndexInSingleBatch, defaultInitialNumberOfItemsToIndexInSingleBatch);

			IndexAndTransformerReplicationLatencyInSec = ravenSettings.IndexAndTransformerReplicationLatencyInSec.Value;
			BulkImportBatchTimeout = ravenSettings.BulkImportBatchTimeout.Value;

			// Important! this value is synchronized with the max sessions number in esent
			// since we cannot have more requests in the system than we have sessions for them
			// and we also need to allow sessions for background operations and for multi get requests
			MaxConcurrentServerRequests = ravenSettings.MaxConcurrentServerRequests.Value;

			MaxConcurrentRequestsForDatabaseDuringLoad = ravenSettings.MaxConcurrentRequestsForDatabaseDuringLoad.Value;

            MaxSecondsForTaskToWaitForDatabaseToLoad = ravenSettings.MaxSecondsForTaskToWaitForDatabaseToLoad.Value;
			MaxConcurrentMultiGetRequests = ravenSettings.MaxConcurrentMultiGetRequests.Value;
			if (ConcurrentMultiGetRequests == null)
				ConcurrentMultiGetRequests = new SemaphoreSlim(MaxConcurrentMultiGetRequests);

			MemoryLimitForProcessingInMb = ravenSettings.MemoryLimitForProcessing.Value;

			PrefetchingDurationLimit = ravenSettings.PrefetchingDurationLimit.Value;

			// Core settings
			MaxPageSize = ravenSettings.MaxPageSize.Value;

			MemoryCacheLimitMegabytes = ravenSettings.MemoryCacheLimitMegabytes.Value;

			MemoryCacheExpiration = ravenSettings.MemoryCacheExpiration.Value;

			MemoryCacheLimitPercentage = ravenSettings.MemoryCacheLimitPercentage.Value;

			MemoryCacheLimitCheckInterval = ravenSettings.MemoryCacheLimitCheckInterval.Value;

			// Discovery
			DisableClusterDiscovery = ravenSettings.DisableClusterDiscovery.Value;

			ServerName = ravenSettings.ServerName.Value;

			MaxStepsForScript = ravenSettings.MaxStepsForScript.Value;
			AdditionalStepsForScriptBasedOnDocumentSize = ravenSettings.AdditionalStepsForScriptBasedOnDocumentSize.Value;

			// Index settings
			MaxProcessingRunLatency = ravenSettings.MaxProcessingRunLatency.Value;
			MaxIndexWritesBeforeRecreate = ravenSettings.MaxIndexWritesBeforeRecreate.Value;
			MaxSimpleIndexOutputsPerDocument = ravenSettings.MaxSimpleIndexOutputsPerDocument.Value;
			MaxMapReduceIndexOutputsPerDocument = ravenSettings.MaxMapReduceIndexOutputsPerDocument.Value;

		    PrewarmFacetsOnIndexingMaxAge = ravenSettings.PrewarmFacetsOnIndexingMaxAge.Value;
		    PrewarmFacetsSyncronousWaitTime = ravenSettings.PrewarmFacetsSyncronousWaitTime.Value;

			MaxNumberOfItemsToProcessInSingleBatch = ravenSettings.MaxNumberOfItemsToProcessInSingleBatch.Value;
			FlushIndexToDiskSizeInMb = ravenSettings.FlushIndexToDiskSizeInMb.Value;

			var initialNumberOfItemsToIndexInSingleBatch = Settings["Raven/InitialNumberOfItemsToProcessInSingleBatch"] ?? Settings["Raven/InitialNumberOfItemsToIndexInSingleBatch"];
			if (initialNumberOfItemsToIndexInSingleBatch != null)
			{
				InitialNumberOfItemsToProcessInSingleBatch = Math.Min(int.Parse(initialNumberOfItemsToIndexInSingleBatch),
																	MaxNumberOfItemsToProcessInSingleBatch);
			}
			else
			{
				InitialNumberOfItemsToProcessInSingleBatch = MaxNumberOfItemsToProcessInSingleBatch == ravenSettings.MaxNumberOfItemsToProcessInSingleBatch.Default ?
				 defaultInitialNumberOfItemsToIndexInSingleBatch :
				 Math.Max(16, Math.Min(MaxNumberOfItemsToProcessInSingleBatch / 256, defaultInitialNumberOfItemsToIndexInSingleBatch));
			}
			AvailableMemoryForRaisingBatchSizeLimit = ravenSettings.AvailableMemoryForRaisingBatchSizeLimit.Value;

			MaxNumberOfItemsToReduceInSingleBatch = ravenSettings.MaxNumberOfItemsToReduceInSingleBatch.Value;
			InitialNumberOfItemsToReduceInSingleBatch = MaxNumberOfItemsToReduceInSingleBatch == ravenSettings.MaxNumberOfItemsToReduceInSingleBatch.Default ?
				 defaultInitialNumberOfItemsToIndexInSingleBatch / 2 :
				 Math.Max(16, Math.Min(MaxNumberOfItemsToProcessInSingleBatch / 256, defaultInitialNumberOfItemsToIndexInSingleBatch / 2));

			NumberOfItemsToExecuteReduceInSingleStep = ravenSettings.NumberOfItemsToExecuteReduceInSingleStep.Value;

			var initialNumberOfItemsToReduceInSingleBatch = Settings["Raven/InitialNumberOfItemsToReduceInSingleBatch"];
			if (initialNumberOfItemsToReduceInSingleBatch != null)
			{
				InitialNumberOfItemsToReduceInSingleBatch = Math.Min(int.Parse(initialNumberOfItemsToReduceInSingleBatch),
																	MaxNumberOfItemsToReduceInSingleBatch);
			}

			MaxNumberOfParallelProcessingTasks = ravenSettings.MaxNumberOfParallelProcessingTasks.Value;

			NewIndexInMemoryMaxBytes = ravenSettings.NewIndexInMemoryMaxMb.Value;

			NewIndexInMemoryMaxTime = ravenSettings.NewIndexInMemoryMaxTime.Value;

			MaxIndexCommitPointStoreTimeInterval = ravenSettings.MaxIndexCommitPointStoreTimeInterval.Value;

			MinIndexingTimeIntervalToStoreCommitPoint = ravenSettings.MinIndexingTimeIntervalToStoreCommitPoint.Value;

			MaxNumberOfStoredCommitPoints = ravenSettings.MaxNumberOfStoredCommitPoints.Value;

			// Data settings
			RunInMemory = ravenSettings.RunInMemory.Value;

			if (string.IsNullOrEmpty(DefaultStorageTypeName))
			{
				DefaultStorageTypeName = ravenSettings.DefaultStorageTypeName.Value;
			}

			CreateAutoIndexesForAdHocQueriesIfNeeded = ravenSettings.CreateAutoIndexesForAdHocQueriesIfNeeded.Value;

			DatabaseOperationTimeout = ravenSettings.DatbaseOperationTimeout.Value;

			TimeToWaitBeforeRunningIdleIndexes = ravenSettings.TimeToWaitBeforeRunningIdleIndexes.Value;
			TimeToWaitBeforeMarkingAutoIndexAsIdle = ravenSettings.TimeToWaitBeforeMarkingAutoIndexAsIdle.Value;

			TimeToWaitBeforeMarkingIdleIndexAsAbandoned = ravenSettings.TimeToWaitBeforeMarkingIdleIndexAsAbandoned.Value;
			TimeToWaitBeforeRunningAbandonedIndexes = ravenSettings.TimeToWaitBeforeRunningAbandonedIndexes.Value;

			ResetIndexOnUncleanShutdown = ravenSettings.ResetIndexOnUncleanShutdown.Value;
			DisableInMemoryIndexing = ravenSettings.DisableInMemoryIndexing.Value;

			SetupTransactionMode();

			DataDirectory = ravenSettings.DataDir.Value;
			CountersDataDirectory = ravenSettings.CountersDataDir.Value;

			var indexStoragePathSettingValue = ravenSettings.IndexStoragePath.Value;
			if (string.IsNullOrEmpty(indexStoragePathSettingValue) == false)
			{
				IndexStoragePath = indexStoragePathSettingValue;
			}

			MaxRecentTouchesToRemember = ravenSettings.MaxRecentTouchesToRemember.Value;

			// HTTP settings
			HostName = ravenSettings.HostName.Value;

			ExposeConfigOverTheWire = ravenSettings.ExposeConfigOverTheWire.Value;

			if (string.IsNullOrEmpty(DatabaseName)) // we only use this for root database
			{
				Port = PortUtil.GetPort(ravenSettings.Port.Value, RunInMemory);
				Encryption.UseSsl = ravenSettings.Encryption.UseSsl.Value;
				Encryption.UseFips = ravenSettings.Encryption.UseFips.Value;
			}

			SetVirtualDirectory();

			HttpCompression = ravenSettings.HttpCompression.Value;

			AccessControlAllowOrigin = ravenSettings.AccessControlAllowOrigin.Value == null ? new HashSet<string>() : new HashSet<string>(ravenSettings.AccessControlAllowOrigin.Value.Split());
			AccessControlMaxAge = ravenSettings.AccessControlMaxAge.Value;
			AccessControlAllowMethods = ravenSettings.AccessControlAllowMethods.Value;
			AccessControlRequestHeaders = ravenSettings.AccessControlRequestHeaders.Value;

			AnonymousUserAccessMode = GetAnonymousUserAccessMode();

			RedirectStudioUrl = ravenSettings.RedirectStudioUrl.Value;

			DisableDocumentPreFetching = ravenSettings.DisableDocumentPreFetching.Value;

			MaxNumberOfItemsToPreFetch = ravenSettings.MaxNumberOfItemsToPreFetch.Value;
			
			// Misc settings
			WebDir = ravenSettings.WebDir.Value;

			PluginsDirectory = ravenSettings.PluginsDirectory.Value.ToFullPath();

		    AssembliesDirectory = ravenSettings.AssembliesDirectory.Value.ToFullPath();

		    EmbeddedFilesDirectory = ravenSettings.EmbeddedFilesDirectory.Value.ToFullPath();

			CompiledIndexCacheDirectory = ravenSettings.CompiledIndexCacheDirectory.Value.ToFullTempPath();

			var taskSchedulerType = ravenSettings.TaskScheduler.Value;
			if (taskSchedulerType != null)
			{
				var type = Type.GetType(taskSchedulerType);
				CustomTaskScheduler = (TaskScheduler)Activator.CreateInstance(type);
			}

			AllowLocalAccessWithoutAuthorization = ravenSettings.AllowLocalAccessWithoutAuthorization.Value;
		    RejectClientsMode = ravenSettings.RejectClientsModeEnabled.Value;

		    Storage.Voron.MaxBufferPoolSize = Math.Max(2, ravenSettings.Voron.MaxBufferPoolSize.Value);
			Storage.Voron.InitialFileSize = ravenSettings.Voron.InitialFileSize.Value;
			Storage.Voron.MaxScratchBufferSize = ravenSettings.Voron.MaxScratchBufferSize.Value;
			Storage.Voron.AllowIncrementalBackups = ravenSettings.Voron.AllowIncrementalBackups.Value;
			Storage.Voron.TempPath = ravenSettings.Voron.TempPath.Value;
			Storage.Voron.JournalsStoragePath = ravenSettings.Voron.JournalsStoragePath.Value;

			Storage.Esent.JournalsStoragePath = ravenSettings.Esent.JournalsStoragePath.Value;

			Prefetcher.FetchingDocumentsFromDiskTimeoutInSeconds = ravenSettings.Prefetcher.FetchingDocumentsFromDiskTimeoutInSeconds.Value;
			Prefetcher.MaximumSizeAllowedToFetchFromStorageInMb = ravenSettings.Prefetcher.MaximumSizeAllowedToFetchFromStorageInMb.Value;

			Replication.FetchingFromDiskTimeoutInSeconds = ravenSettings.Replication.FetchingFromDiskTimeoutInSeconds.Value;
			Replication.ReplicationRequestTimeoutInMilliseconds = ravenSettings.Replication.ReplicationRequestTimeoutInMilliseconds.Value;
			Replication.MaxNumberOfItemsToReceiveInSingleBatch = ravenSettings.Replication.MaxNumberOfItemsToReceiveInSingleBatch.Value;

            FileSystem.MaximumSynchronizationInterval = ravenSettings.FileSystem.MaximumSynchronizationInterval.Value;
			FileSystem.DataDirectory = ravenSettings.FileSystem.DataDir.Value;
			FileSystem.IndexStoragePath = ravenSettings.FileSystem.IndexStoragePath.Value;

			if (string.IsNullOrEmpty(FileSystem.DefaultStorageTypeName))
				FileSystem.DefaultStorageTypeName = ravenSettings.FileSystem.DefaultStorageTypeName.Value;

			Encryption.EncryptionKeyBitsPreference = ravenSettings.Encryption.EncryptionKeyBitsPreference.Value;

			Indexing.MaxNumberOfItemsToProcessInTestIndexes = ravenSettings.Indexing.MaxNumberOfItemsToProcessInTestIndexes.Value;

			TombstoneRetentionTime = ravenSettings.TombstoneRetentionTime.Value;

			IgnoreSslCertificateErros = GetIgnoreSslCertificateErrorModeMode();

			PostInit();

			return this;
		}

	    public int MaxSecondsForTaskToWaitForDatabaseToLoad { get; set; }

	    public int IndexAndTransformerReplicationLatencyInSec { get; internal set; }

		/// <summary>
		/// Determines how long replication and periodic backup tombstones will be kept by a database. After the specified time they will be automatically
		/// purged on next database startup. Default: 14 days.
		/// </summary>
		public TimeSpan TombstoneRetentionTime { get; set; }

		public int MaxConcurrentServerRequests { get; set; }

		public int MaxConcurrentRequestsForDatabaseDuringLoad { get; set; }

		public int MaxConcurrentMultiGetRequests { get; set; }

		public int PrefetchingDurationLimit { get; private set; }

		public TimeSpan BulkImportBatchTimeout { get; set; }

		/// <summary>
        /// This limits the number of concurrent multi get requests,
        /// Note that this plays with the max number of requests allowed as well as the max number
        /// of sessions
        /// </summary>
		[JsonIgnore]
		public SemaphoreSlim ConcurrentMultiGetRequests;

		/// <summary>
		/// The time to wait before canceling a database operation such as load (many) or query
		/// </summary>
		public TimeSpan DatabaseOperationTimeout { get; private set; }

		public TimeSpan TimeToWaitBeforeRunningIdleIndexes { get; internal set; }

		public TimeSpan TimeToWaitBeforeRunningAbandonedIndexes { get; private set; }

		public TimeSpan TimeToWaitBeforeMarkingAutoIndexAsIdle { get; private set; }

		public TimeSpan TimeToWaitBeforeMarkingIdleIndexAsAbandoned { get; private set; }

		private void FilterActiveBundles()
		{
			if (container != null)
				container.Dispose();
			container = null;

			var catalog = GetUnfilteredCatalogs(Catalog.Catalogs);
			Catalog = new AggregateCatalog(new List<ComposablePartCatalog> { new BundlesFilteredCatalog(catalog, ActiveBundles.ToArray()) });
		}

		public IEnumerable<string> ActiveBundles
		{
			get
			{
				var activeBundles = Settings[Constants.ActiveBundles] ?? string.Empty;

				return BundlesHelper.ProcessActiveBundles(activeBundles)
					.GetSemicolonSeparatedValues()
					.Distinct();
			}
		}

		private HashSet<string> headersToIgnore;
		public HashSet<string> HeadersToIgnore
		{
			get
			{
				if (headersToIgnore != null)
					return headersToIgnore;

				var headers = Settings["Raven/Headers/Ignore"] ?? string.Empty;
				return headersToIgnore = new HashSet<string>(headers.GetSemicolonSeparatedValues(), StringComparer.OrdinalIgnoreCase);
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

		private static readonly Lazy<byte[]> DefaultOauthKey = new Lazy<byte[]>(() =>
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
			return DefaultOauthKey.Value; // ensure we only create this once per process
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
				return new UriBuilder(Encryption.UseSsl ? "https" : "http", (HostName ?? Environment.MachineName), Port, VirtualDirectory).Uri.ToString();
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
		public IIndexingClassifier IndexingClassifier { get; set; }

		/// <summary>
		/// Max number of items to take for indexing in a batch
		/// Minimum: 128
		/// </summary>
		public int MaxNumberOfItemsToProcessInSingleBatch { get; set; }

		/// <summary>
		/// The initial number of items to take when processing a batch
		/// Default: 512 or 256 depending on CPU architecture
		/// </summary>
		public int InitialNumberOfItemsToProcessInSingleBatch { get; set; }

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
		/// The maximum number of indexing, replication and sql replication tasks allowed to run in parallel
		/// Default: The number of processors in the current machine
		/// </summary>
		public int MaxNumberOfParallelProcessingTasks
		{
			get
			{
				if (MemoryStatistics.MaxParallelismSet)
					return Math.Min(maxNumberOfParallelIndexTasks ?? MemoryStatistics.MaxParallelism, MemoryStatistics.MaxParallelism);
				return maxNumberOfParallelIndexTasks ?? Environment.ProcessorCount;
			}
			set
			{
				if (value == 0)
					throw new ArgumentException("You cannot set the number of parallel tasks to zero");
				maxNumberOfParallelIndexTasks = value;
			}
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
		/// The port to use when creating the http listener. 
		/// Default: 8080. You can set it to *, in which case it will find the first available port from 8080 and upward.
		/// </summary>
		public int Port { get; set; }

		/// <summary>
		/// Allow to get config information over the wire.
		/// Applies to endpoints: /debug/config, /debug...
		/// Default: Open. You can set it to AdminOnly.
		/// </summary>
		public string ExposeConfigOverTheWire { get; set; }

		/// <summary>
		/// Determine the value of the Access-Control-Allow-Origin header sent by the server. 
		/// Indicates the URL of a site trusted to make cross-domain requests to this server.
		/// Allowed values: null (don't send the header), *, http://example.org (space separated if multiple sites)
		/// </summary>
		public HashSet<string> AccessControlAllowOrigin { get; set; }

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
        /// If set all client request to the server will be rejected with 
        /// the http 503 response.
        /// Other servers or the studio could still access the server.
        /// </summary>
        public bool RejectClientsMode { get; set; }

		/// <summary>
		/// The certificate to use when verifying access token signatures for OAuth
		/// </summary>
		public byte[] OAuthTokenKey { get; set; }

		public IgnoreSslCertificateErrorsMode IgnoreSslCertificateErros { get; set; }

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
			set { dataDirectory = value == null ? null : FilePathTools.MakeSureEndsWithSlash(value.ToFullPath()); }
		}

		/// <summary>
		/// The directory for the RavenDB counters. 
		/// You can use the ~\ prefix to refer to RavenDB's base directory. 
		/// </summary>
		public string CountersDataDirectory
		{
			get { return countersDataDirectory; }
			set { countersDataDirectory = value == null ? null : FilePathTools.MakeSureEndsWithSlash(value.ToFullPath()); }
		}

		/// <summary>
		/// What storage type to use (see: RavenDB Storage engines)
		/// Allowed values: esent, voron
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
		/// Should RavenDB's storage be in-memory. If set to true, Voron would be used as the
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
                Settings[Constants.RunInMemory] = value.ToString();
			}
		}

		/// <summary>
		/// Prevent index from being kept in memory. Default: false
		/// </summary>
		public bool DisableInMemoryIndexing { get; set; }

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
					.Concat(Catalog.Catalogs.OfType<Plugins.Catalogs.FilteredCatalog>()
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

        /// <summary>
        /// Where the internal assemblies will be extracted to.
        /// Default: ~\Assemblies
        /// </summary>
        public string AssembliesDirectory { get; set; }

        /// <summary>
        /// Where we search for embedded files.
        /// Default: null
        /// </summary>
        public string EmbeddedFilesDirectory { get; set; }

		public bool CreatePluginsDirectoryIfNotExisting { get; set; }
		public bool CreateAnalyzersDirectoryIfNotExisting { get; set; }

		/// <summary>
		/// Where to cache the compiled indexes. Absolute path or relative to TEMP directory.
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

		public bool DisableDocumentPreFetching { get; set; }

		public int MaxNumberOfItemsToPreFetch { get; set; }

		[JsonIgnore]
		public AggregateCatalog Catalog { get; set; }

		public bool RunInUnreliableYetFastModeThatIsNotSuitableForProduction { get; set; }

		private string indexStoragePath, journalStoragePath;
		
		private string countersDataDirectory;
		private int? maxNumberOfParallelIndexTasks;

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

		/// <summary>
		/// Limit of how much memory a batch processing can take (in MBytes)
		/// </summary>
		public int MemoryLimitForProcessingInMb { get; set; }

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

		public int AvailableMemoryForRaisingBatchSizeLimit { get; set; }

		public TimeSpan MaxProcessingRunLatency { get; set; }

		internal bool IsTenantDatabase { get; set; }
		
		/// <summary>
		/// If True, cluster discovery will be disabled. Default is False
		/// </summary>
		public bool DisableClusterDiscovery { get; set; }

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
		/// The maximum number of recent document touches to store (i.e. updates done in
		/// order to initiate indexing rather than because something has actually changed).
		/// </summary>
		public int MaxRecentTouchesToRemember { get; set; }

		/// <summary>
		/// The number of additional steps to add to a given script based on the processed document's quota.
		/// Set to 0 to give use a fixed size quota. This value is multiplied with the doucment size.
		/// Default: 5
		/// </summary>
		public int AdditionalStepsForScriptBasedOnDocumentSize { get; set; }

		public int MaxIndexWritesBeforeRecreate { get; set; }

		/// <summary>
		/// Limits the number of map outputs that a simple index is allowed to create for a one source document. If a map operation applied to the one document
		/// produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and
		/// the appropriate error message will be added to the indexing errors.
		/// Default value: 15. In order to disable this check set value to -1.
		/// </summary>
		public int MaxSimpleIndexOutputsPerDocument { get; set; }

		/// <summary>
		/// Limits the number of map outputs that a map-reduce index is allowed to create for a one source document. If a map operation applied to the one document
		/// produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and
		/// the appropriate error message will be added to the indexing errors.
		/// Default value: 50. In order to disable this check set value to -1.
		/// </summary>
		public int MaxMapReduceIndexOutputsPerDocument { get; set; }

		/// <summary>
        /// What is the maximum age of a facet query that we should consider when prewarming
        /// the facet cache when finishing an indexing batch
        /// </summary>
		[Browsable(false)]
	    public TimeSpan PrewarmFacetsOnIndexingMaxAge { get; set; }
	    
        /// <summary>
        /// The time we should wait for pre-warming the facet cache from existing query after an indexing batch
        /// in a syncronous manner (after that, the pre warm still runs, but it will do so in a background thread).
        /// Facet queries that will try to use it will have to wait until it is over
        /// </summary>
        public TimeSpan PrewarmFacetsSyncronousWaitTime { get; set; }

		/// <summary>
		/// Indexes are flushed to a disk only if their in-memory size exceed the specified value. Default: 5MB
		/// </summary>
		public long FlushIndexToDiskSizeInMb { get; set; }

		public bool EnableResponseLoggingForEmbeddedDatabases { get; set; }

		/// <summary>
		/// How long can we keep the new index in memory before we have to flush it
		/// </summary>
		public TimeSpan NewIndexInMemoryMaxTime { get; set; }

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
				containerExternallySet = false;
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

		protected IgnoreSslCertificateErrorsMode GetIgnoreSslCertificateErrorModeMode()
		{
			if (string.IsNullOrEmpty(Settings["Raven/IgnoreSslCertificateErrors"]) == false)
			{
				var val = Enum.Parse(typeof(IgnoreSslCertificateErrorsMode), Settings["Raven/IgnoreSslCertificateErrors"]);
				return (IgnoreSslCertificateErrorsMode)val;
			}
			return IgnoreSslCertificateErrorsMode.None;
		}

		public Uri GetFullUrl(string baseUrl)
		{
			baseUrl = Uri.EscapeUriString(baseUrl);

			if (baseUrl.StartsWith("/"))
				baseUrl = baseUrl.Substring(1);

			var url = VirtualDirectory.EndsWith("/") ? VirtualDirectory + baseUrl : VirtualDirectory + "/" + baseUrl;
			return new Uri(url, UriKind.RelativeOrAbsolute);
		}

		public T? GetConfigurationValue<T>(string configName) where T : struct
		{
			// explicitly fail if we can't convert it
			if (string.IsNullOrEmpty(Settings[configName]) == false)
				return (T)Convert.ChangeType(Settings[configName], typeof(T));
			return null;
		}

		[CLSCompliant(false)]
		public ITransactionalStorage CreateTransactionalStorage(string storageEngine, Action notifyAboutWork, Action handleStorageInaccessible)
		{
			storageEngine = StorageEngineAssemblyNameByTypeName(storageEngine);
			var type = Type.GetType(storageEngine);

			if (type == null)
				throw new InvalidOperationException("Could not find transactional storage type: " + storageEngine);

			return (ITransactionalStorage)Activator.CreateInstance(type, this, notifyAboutWork, handleStorageInaccessible);
		}


	    public static string StorageEngineAssemblyNameByTypeName(string typeName)
		{
	        switch (typeName.ToLowerInvariant())
	        {
	            case EsentTypeName:
					typeName = typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;
	                break;
	            case VoronTypeName:
					typeName = typeof(Raven.Storage.Voron.TransactionalStorage).AssemblyQualifiedName;
	                break;
                default:
					throw new ArgumentException("Invalid storage engine type name: " + typeName);
	        }
	        return typeName;
	    }	  

	    public string SelectStorageEngineAndFetchTypeName()
		{
			if (RunInMemory)
			{
				if (!string.IsNullOrWhiteSpace(DefaultStorageTypeName) &&
				    DefaultStorageTypeName.Equals(EsentTypeName, StringComparison.InvariantCultureIgnoreCase))
					return EsentTypeName;
                return VoronTypeName;                
			}

			if (String.IsNullOrEmpty(DataDirectory) == false && Directory.Exists(DataDirectory))
			{
				if (File.Exists(Path.Combine(DataDirectory, Voron.Impl.Constants.DatabaseFilename)))
				{
                    return VoronTypeName;
				}
				if (File.Exists(Path.Combine(DataDirectory, "Data")))
					return EsentTypeName;
			}

		    if (string.IsNullOrEmpty(DefaultStorageTypeName))
			    return EsentTypeName;
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

        public void CustomizeValuesForDatabaseTenant(string tenantId)
        {
            if (string.IsNullOrEmpty(Settings["Raven/IndexStoragePath"]) == false)
                Settings["Raven/IndexStoragePath"] = Path.Combine(Settings["Raven/IndexStoragePath"], "Databases", tenantId);

            if (string.IsNullOrEmpty(Settings["Raven/Esent/LogsPath"]) == false)
                Settings["Raven/Esent/LogsPath"] = Path.Combine(Settings["Raven/Esent/LogsPath"], "Databases", tenantId);

            if (string.IsNullOrEmpty(Settings[Constants.RavenTxJournalPath]) == false)
                Settings[Constants.RavenTxJournalPath] = Path.Combine(Settings[Constants.RavenTxJournalPath], "Databases", tenantId);

            if (string.IsNullOrEmpty(Settings["Raven/Voron/TempPath"]) == false)
                Settings["Raven/Voron/TempPath"] = Path.Combine(Settings["Raven/Voron/TempPath"], "Databases", tenantId, "VoronTemp");
        }

        public void CustomizeValuesForFileSystemTenant(string tenantId)
        {                                             
            if (string.IsNullOrEmpty(Settings[Constants.FileSystem.DataDirectory]) == false)
                Settings[Constants.FileSystem.DataDirectory] = Path.Combine(Settings[Constants.FileSystem.DataDirectory], "FileSystems", tenantId);
        }

		public void CopyParentSettings(InMemoryRavenConfiguration defaultConfiguration)
		{
			Port = defaultConfiguration.Port;
			OAuthTokenKey = defaultConfiguration.OAuthTokenKey;
			OAuthTokenServer = defaultConfiguration.OAuthTokenServer;

            FileSystem.MaximumSynchronizationInterval = defaultConfiguration.FileSystem.MaximumSynchronizationInterval;

		    Encryption.UseSsl = defaultConfiguration.Encryption.UseSsl;
		    Encryption.UseFips = defaultConfiguration.Encryption.UseFips;

		    AssembliesDirectory = defaultConfiguration.AssembliesDirectory;
		}

		public IEnumerable<string> GetConfigOptionsDocs()
		{
			return ConfigOptionDocs.OptionsDocs;
		}

		public class StorageConfiguration
		{
			public StorageConfiguration()
			{
				Voron = new VoronConfiguration();
				Esent = new EsentConfiguration();
	        }

			public VoronConfiguration Voron { get; private set; }

			public EsentConfiguration Esent { get; private set; }

			public class EsentConfiguration
			{
				public string JournalsStoragePath { get; set; }
			}

			public class VoronConfiguration
			{
				/// <summary>
				/// You can use this setting to specify a maximum buffer pool size that can be used for transactional storage (in gigabytes). 
				/// By default it is 4.
				/// Minimum value is 2.
				/// </summary>
				public int MaxBufferPoolSize { get; set; }

				/// <summary>
				/// You can use this setting to specify an initial file size for data file (in bytes).
				/// </summary>
				public int? InitialFileSize { get; set; }

				/// <summary>
				/// The maximum scratch buffer size that can be used by Voron. The value is in megabytes. 
				/// Default: 512.
				/// </summary>
				public int MaxScratchBufferSize { get; set; }

				/// <summary>
				/// If you want to use incremental backups, you need to turn this to true, but then journal files will not be deleted after applying them to the data file. They will be deleted only after a successful backup. 
				/// Default: false.
				/// </summary>
				public bool AllowIncrementalBackups { get; set; }

				/// <summary>
				/// You can use this setting to specify a different path to temporary files. By default it is empty, which means that temporary files will be created at same location as data file.
				/// </summary>
				public string TempPath { get; set; }

				public string JournalsStoragePath { get; set; }
			}
		}

		public class PrefetcherConfiguration
		{
			/// <summary>
			/// Number of seconds after which prefetcher will stop reading documents from disk. Default: 5.
			/// </summary>
			public int FetchingDocumentsFromDiskTimeoutInSeconds { get; set; }

			/// <summary>
			/// Maximum number of megabytes after which prefetcher will stop reading documents from disk. Default: 256.
			/// </summary>
			public int MaximumSizeAllowedToFetchFromStorageInMb { get; set; }
		}

		public class ReplicationConfiguration
		{
			/// <summary>
			/// Number of seconds after which replication will stop reading documents/attachments from disk. Default: 30.
			/// </summary>
			public int FetchingFromDiskTimeoutInSeconds { get; set; }

			/// <summary>
			/// Number of milliseconds before replication requests will timeout. Default: 60 * 1000.
			/// </summary>
			public int ReplicationRequestTimeoutInMilliseconds { get; set; }

			/// <summary>
			/// Maximum number of items replication will receive in single batch. Min: 512. Default: null (let source server decide).
			/// </summary>
			public int? MaxNumberOfItemsToReceiveInSingleBatch { get; set; }
		}

        public class FileSystemConfiguration
        {
			private string fileSystemDataDirectory;

			private string fileSystemIndexStoragePath;

			private string defaultFileSystemStorageTypeName;

            public TimeSpan MaximumSynchronizationInterval { get; set; }

			/// <summary>
			/// The directory for the RavenDB file system. 
			/// You can use the ~\ prefix to refer to RavenDB's base directory. 
			/// </summary>
			public string DataDirectory
			{
				get { return fileSystemDataDirectory; }
				set { fileSystemDataDirectory = value == null ? null : FilePathTools.MakeSureEndsWithSlash(value.ToFullPath()); }
			}

			public string IndexStoragePath
			{
				get
				{
					if (string.IsNullOrEmpty(fileSystemIndexStoragePath))
						fileSystemIndexStoragePath = Path.Combine(DataDirectory, "Indexes");
					return fileSystemIndexStoragePath;
				}
				set { fileSystemIndexStoragePath = value.ToFullPath(); }
			}

			/// <summary>
			/// What storage type to use in RavenFS (see: RavenFS Storage engines)
			/// Allowed values: esent, voron
			/// Default: esent
			/// </summary>
			public string DefaultStorageTypeName
			{
				get { return defaultFileSystemStorageTypeName; }
				set { if (!string.IsNullOrEmpty(value)) defaultFileSystemStorageTypeName = value; }
			}
        }

		public class EncryptionConfiguration
		{
			/// <summary>
			/// Whatever we should use FIPS compliant encryption algorithms
			/// </summary>
			public bool UseFips { get; set; }

			public int EncryptionKeyBitsPreference { get; set; }

			/// <summary>
			/// Whatever we should use SSL for this connection
			/// </summary>
			public bool UseSsl { get; set; }
		}

		public class IndexingConfiguration
		{
			public int MaxNumberOfItemsToProcessInTestIndexes { get; set; }
		}

		public void UpdateDataDirForLegacySystemDb()
		{
			if (RunInMemory)
				return;
			var legacyPath = Settings["Raven/DataDir/Legacy"];
			if (string.IsNullOrEmpty(legacyPath))
				return;
			var fullLegacyPath = FilePathTools.MakeSureEndsWithSlash(legacyPath.ToFullPath());

			// if we already have a system database in the legacy path, we want to keep it.
			// The idea is that we don't want to have the user experience "missing databases" because
			// we change the path to make it nicer.
			if (Directory.Exists(fullLegacyPath))
			{
				DataDirectory = legacyPath;
			}
		}
	}
}
