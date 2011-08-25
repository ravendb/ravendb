//-----------------------------------------------------------------------
// <copyright file="InMemoryRavenConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Linq;
using System.Runtime.Caching;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Http;

namespace Raven.Database.Config
{
	public class InMemoryRavenConfiguration : IRavenHttpConfiguration
	{
		private CompositionContainer container;
		private bool containerExternallySet;
		private string dataDirectory;
		private string pluginsDirectory;
		private bool runInUnreliableYetFastModeThatIsNotSuitableForProduction;

		public InMemoryRavenConfiguration()
		{
			Settings = new NameValueCollection(StringComparer.InvariantCultureIgnoreCase);

			BackgroundTasksPriority = ThreadPriority.Normal;
			MaxNumberOfItemsToIndexInSingleBatch = 2500;
			MaxNumberOfParallelIndexTasks = 8;

			Catalog = new AggregateCatalog(
				new AssemblyCatalog(typeof(HttpServer).Assembly),
				new AssemblyCatalog(typeof(DocumentDatabase).Assembly)
				);

			Catalog.Changed += (sender, args) => ResetContainer();
		}

		public void Initialize()
		{
			// Core settings
			var maxPageSizeStr = Settings["Raven/MaxPageSize"];
			MaxPageSize = maxPageSizeStr != null ? int.Parse(maxPageSizeStr) : 1024;
			MaxPageSize = Math.Max(MaxPageSize, 10);

			var backgroundTasksPriority = Settings["Raven/BackgroundTasksPriority"];
			BackgroundTasksPriority = backgroundTasksPriority == null
								? ThreadPriority.Normal
								: (ThreadPriority)Enum.Parse(typeof(ThreadPriority), backgroundTasksPriority);

			var cacheMemoryLimitMegabytes = Settings["Raven/MemoryCacheLimitMegabytes"];
			MemoryCacheLimitMegabytes = cacheMemoryLimitMegabytes == null
											? GetDefaultMemoryCacheLimitMegabytes()
			                            	: int.Parse(cacheMemoryLimitMegabytes);

			

			var memoryCacheLimitPercentage = Settings["Raven/MemoryCacheLimitPercentage"];
			MemoryCacheLimitPercentage = memoryCacheLimitPercentage == null
								? 0 // auto-size
								: int.Parse(memoryCacheLimitPercentage);
			var memoryCacheLimitCheckInterval = Settings["Raven/MemoryCacheLimitCheckInterval"];
			MemoryCacheLimitCheckInterval = memoryCacheLimitCheckInterval == null
								? MemoryCache.Default.PollingInterval
								: TimeSpan.Parse(memoryCacheLimitCheckInterval);

			// Index settings
			var maxNumberOfItemsToIndexInSingleBatch = Settings["Raven/MaxNumberOfItemsToIndexInSingleBatch"];
			MaxNumberOfItemsToIndexInSingleBatch = maxNumberOfItemsToIndexInSingleBatch != null ? int.Parse(maxNumberOfItemsToIndexInSingleBatch) : 2500;
			MaxNumberOfItemsToIndexInSingleBatch = Math.Max(MaxNumberOfItemsToIndexInSingleBatch, 128);

			var maxNumberOfParallelIndexTasks = Settings["Raven/MaxNumberOfParallelIndexTasks"];
			MaxNumberOfParallelIndexTasks = maxNumberOfParallelIndexTasks != null ? int.Parse(maxNumberOfParallelIndexTasks) : Environment.ProcessorCount;
			MaxNumberOfParallelIndexTasks = Math.Max(1, MaxNumberOfParallelIndexTasks);

			var minimumQueryCount = Settings["Raven/TempIndexPromotionMinimumQueryCount"];
			TempIndexPromotionMinimumQueryCount = minimumQueryCount != null ? int.Parse(minimumQueryCount) : 100;

			var queryThreshold = Settings["Raven/TempIndexPromotionThreshold"];
			TempIndexPromotionThreshold = queryThreshold != null ? int.Parse(queryThreshold) : 60000; // once a minute

			var cleanupPeriod = Settings["Raven/TempIndexCleanupPeriod"];
			TempIndexCleanupPeriod = cleanupPeriod != null ? TimeSpan.FromSeconds(int.Parse(cleanupPeriod)) : TimeSpan.FromMinutes(10);

			var cleanupThreshold = Settings["Raven/TempIndexCleanupThreshold"];
			TempIndexCleanupThreshold = cleanupThreshold != null ? TimeSpan.FromSeconds(int.Parse(cleanupThreshold)) : TimeSpan.FromMinutes(20);

			var tempMemoryMaxMb = Settings["Raven/TempIndexInMemoryMaxMB"];
			TempIndexInMemoryMaxBytes = tempMemoryMaxMb != null ? int.Parse(tempMemoryMaxMb) * 1024000 : 26214400;
			TempIndexInMemoryMaxBytes = Math.Max(1024000, TempIndexInMemoryMaxBytes);

			// Data settings
			RunInMemory = GetConfigurationValue<bool>("Raven/RunInMemory") ?? false;
			DefaultStorageTypeName = Settings["Raven/StorageTypeName"] ?? Settings["Raven/StorageEngine"] ?? "esent";

			var transactionMode = Settings["Raven/TransactionMode"];
			TransactionMode result;
			if (Enum.TryParse(transactionMode, true, out result) == false)
				result = TransactionMode.Safe;
			TransactionMode = result;

			DataDirectory = Settings["Raven/DataDir"] ?? @"~\Data";
			if (string.IsNullOrEmpty(Settings["Raven/IndexStoragePath"]) == false)
			{
				IndexStoragePath = Settings["Raven/IndexStoragePath"];
			}

			// HTTP settings
			HostName = Settings["Raven/HostName"];
			Port = PortUtil.GetPort(Settings["Raven/Port"]);
			VirtualDirectory = Settings["Raven/VirtualDirectory"] ?? "/";

			if (VirtualDirectory.EndsWith("/"))
				VirtualDirectory = VirtualDirectory.Substring(0, VirtualDirectory.Length - 1);
			if (VirtualDirectory.StartsWith("/") == false)
				VirtualDirectory = "/" + VirtualDirectory;

			bool httpCompressionTemp;
			if (bool.TryParse(Settings["Raven/HttpCompression"], out httpCompressionTemp) == false)
				httpCompressionTemp = true;
			HttpCompression = httpCompressionTemp;

			AccessControlAllowOrigin = Settings["Raven/AccessControlAllowOrigin"];

			AnonymousUserAccessMode = GetAnonymousUserAccessMode();

			// Misc settings
			WebDir = Settings["Raven/WebDir"] ?? GetDefaultWebDir();

			PluginsDirectory = Settings["Raven/PluginsDirectory"] ?? @"~\Plugins";
			if (PluginsDirectory.StartsWith(@"~\"))
				PluginsDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, PluginsDirectory.Substring(2));

			// OAuth
			AuthenticationMode = Settings["Raven/AuthenticationMode"] ?? "windows";

			if (string.Equals(AuthenticationMode, "oauth", StringComparison.InvariantCultureIgnoreCase) == false) 
				return;
			OAuthTokenServer = Settings["Raven/OAuthTokenServer"] ?? ServerUrl + "/OAuth/AccessToken";
			OAuthTokenCertificate = GetCertificate();
		}

		private X509Certificate2 GetCertificate()
		{
			var path = Settings["Raven/OAuthTokenCertificatePath"];
			if (string.IsNullOrEmpty(path) == false)
			{
				var pwd = Settings["Raven/OAuthTokenCertificatePassword"];
				if (string.IsNullOrEmpty(pwd) == false)
				{
					return new X509Certificate2(path, pwd);
				}
				return new X509Certificate2(path);
			}

			return CertGenerator.GenerateNewCertificate("RavenDB");
		}


		private int GetDefaultMemoryCacheLimitMegabytes()
		{
			var totalPhysicalMemoryMegabytes =
				(int) (new Microsoft.VisualBasic.Devices.ComputerInfo().TotalPhysicalMemory/1024/1024);
			// we need to leave ( a lot ) of room for other things as well, so we limit the cache size

			var val = (totalPhysicalMemoryMegabytes / 2)  - 
						// reduce the unmanaged cache size from the default limit
						(GetConfigurationValue<int>("Raven/Esent/CacheSizeMax") ?? 1024);

			if (val < 0)
				return 128; // if machine has less than 1024 MB, then only use 128 MB 

			return val;
		}

		public NameValueCollection Settings { get; set; }

		public string ServerUrl
		{
			get
			{
				return new UriBuilder("http", (HostName ?? Environment.MachineName), Port, VirtualDirectory).Uri.ToString();
			}
		}

#region Core settings

		/// <summary>
		/// What thread priority to give the various background tasks RavenDB uses (mostly for indexing)
		/// Allowed values: Lowest, BelowNormal, Normal, AboveNormal, Highest
		/// Default: Normal
		/// </summary>
		public ThreadPriority BackgroundTasksPriority { get; set; }

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
		/// Max number of items to take for indexing in a batch
		/// Default: 2500
		/// Minimum: 128
		/// </summary>
		public int MaxNumberOfItemsToIndexInSingleBatch { get; set; }

		/// <summary>
		/// The maximum number of indexing tasks allowed to run in parallel
		/// Default: The number of processors in the current machine
		/// </summary>
		public int MaxNumberOfParallelIndexTasks { get; set; }

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
		/// Default: 8080
		/// </summary>
		public int Port { get; set; }

		/// <summary>
		/// Determine the value of the Access-Control-Allow-Origin header sent by the server. 
		/// Allowed values: null (don't send the header), *, http://example.org 
		/// </summary>
		public string AccessControlAllowOrigin { get; set; }

		/// <summary>
		/// The virtual directory to use when creating the http listener. 
		/// Default: / 
		/// </summary>
		public string VirtualDirectory { get; set; }

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
        /// Defines which mode to use to authenticate requests
        /// Allowed values: Windows, OAuth
        /// Default: Windows
        /// </summary>
        public string AuthenticationMode { get; set; }

        /// <summary>
        /// The certificate to use when verifying access token signatures for OAuth
        /// </summary>
        public X509Certificate2 OAuthTokenCertificate { get; set; }

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
			set { dataDirectory = value.ToFullPath(); }
		}

		/// <summary>
		/// What storage type to use (see: RavenDB Storage engines)
		/// Allowed values: esent, munin
		/// Default: esent
		/// </summary>
		public string DefaultStorageTypeName { get; set; }

		/// <summary>
		/// Should RavenDB's storage be in-memory. If set to true, Munin would be used as the
		/// storage engine, regardless of what was specified for StorageTypeName
		/// Allowed values: true/false
		/// Default: false
		/// </summary>
		public bool RunInMemory { get; set; }

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
				foreach (
					var directoryCatalogToRemove in
						Catalog.Catalogs.OfType<DirectoryCatalog>().Where(c => c.Path == pluginsDirectory).ToArray())
				{
					Catalog.Catalogs.Remove(directoryCatalogToRemove);
				}

				pluginsDirectory = value.ToFullPath();

				// add new one
				if (Directory.Exists(pluginsDirectory))
				{
					Catalog.Catalogs.Add(new DirectoryCatalog(pluginsDirectory));
				}
			}
		}

		public string OAuthTokenServer { get; set; }

		#endregion

		public CompositionContainer Container
		{
			get { return container ?? (container = new CompositionContainer(Catalog)); }
			set
			{
				containerExternallySet = true;
				container = value;
			}
		}

		public AggregateCatalog Catalog { get; set; }

		public bool RunInUnreliableYetFastModeThatIsNotSuitableForProduction
		{
			get { return runInUnreliableYetFastModeThatIsNotSuitableForProduction; }
			set
			{
				RunInMemory = value;
				runInUnreliableYetFastModeThatIsNotSuitableForProduction = value;
			}
		}

		private string indexStoragePath;
		public string IndexStoragePath
		{
			get
			{
				if (string.IsNullOrEmpty(indexStoragePath))
					return Path.Combine(DataDirectory, "Indexes");
				
				return indexStoragePath;
			}
			set
			{
				indexStoragePath = value.ToFullPath();
			}
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
				return (AnonymousUserAccessMode) val;
			}
			return AnonymousUserAccessMode.Get;
		}

		private static string GetDefaultWebDir()
		{
			return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"Raven/WebUI");
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
			// explicitly fail if we can convert it
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
					storageEngine = "Raven.Storage.Esent.TransactionalStorage, Raven.Storage.Esent";
					break;
				case "munin":
					storageEngine = "Raven.Storage.Managed.TransactionalStorage, Raven.Storage.Managed";
					break;
			}
			var type = Type.GetType(storageEngine);

			if (type == null)
				throw new InvalidOperationException("Could not find transactional storage type: " + storageEngine);

			Catalog.Catalogs.Add(new AssemblyCatalog(type.Assembly));

			return (ITransactionalStorage) Activator.CreateInstance(type, this, notifyAboutWork);
		}

		private string SelectStorageEngine()
		{
			if(RunInMemory)
				return "Raven.Storage.Managed.TransactionalStorage, Raven.Storage.Managed";

			if(String.IsNullOrEmpty(DataDirectory) == false && Directory.Exists(DataDirectory))
			{
				if (File.Exists(Path.Combine(DataDirectory, "Raven.ravendb")))
				{
					return "Raven.Storage.Managed.TransactionalStorage, Raven.Storage.Managed";
				}
				if (File.Exists(Path.Combine(DataDirectory, "Data")))
					return "Raven.Storage.Esent.TransactionalStorage, Raven.Storage.Esent";
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
	}
}