using System.Collections;
using System.Collections.Generic;

namespace Raven.Database.Config
{
	public class ConfigOptionDocs : IEnumerable<string>
	{
		public readonly static ConfigOptionDocs OptionsDocs = new ConfigOptionDocs
		{
			// Common
			{"Raven/MaxPageSize", "int", null, "The maximum page size that can be specified on this server, default: 1,024."},
			{"Raven/RunInMemory", "bool", "true,false", "Whatever the database should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing. Default: false."},
			
			// Studio
			{"Raven/RedirectStudioUrl", "string", null, "The url to redirect the user to when then try to access the local studio"},

			// Paths
			{"Raven/DataDir", "string", null,"The path for the database directory. Can use ~\\ as the root, in which case the path will start from the server base directory. Default: ~\\Data."},
			{"Raven/IndexStoragePath", "string", null,"The path for the indexes on disk. Useful if you want to store the indexes on another HD for performance reasons. Default: ~\\Data\\Indexes."},
			{"Raven/Esent/LogsPath", "string", null,"The path for the esent logs. Useful if you want to store the indexes on another HD for performance reasons. Default: ~\\Data\\Logs."},

			// Authentication
			{"Raven/AnonymousAccess", "string", "Get,All,None", "Determines what actions an anonymous user can do. Get - read only, All - read & write, None - allows access to only authenticated users. Default: Get."},
			{"Raven/AuthenticationMode", "string", "windows,oauth", "What authentication mode to use, Windows authentication or OAuth authentication. Default: windows."},
			{"Raven/OAuthTokenServer", "string", null, "The url clients should use for authenticating when using OAuth mode. Default: http://RavenDB-Server-Url/OAuth/AccessToken - the internal OAuth server"},
			{"Raven/OAuthTokenCertificatePath", "string", null, "The path to the OAuth certificate. Default: none. If no certificate is specified, one will be automatically created."},
			{"Raven/OAuthTokenCertificatePassword", "string", null, "The password for the OAuth certificate. Default: none."},
			{"Raven/Authorization/Windows/RequiredGroups", "string", null, "Limit the users that can authenticate to RavenDB to only users in the specified groups. Multiple groups can be specified, separated by a semi column (;)."},
			{"Raven/Authorization/Windows/RequiredUsers", "string", null,  "Limit the users that can authenticate to RavenDB to only the specified users. Multiple users can be specified, separated by a semi column (;)."},
			// Network
			{"Raven/HostName", "string", null, "The hostname to bind the embedded http server to, if we want to bind to a specific hostname, rather than all. Default: none."},
			{"Raven/Port", "int", "1 - 65,536, *", "The port to bind the embedded http server. Default: 8080. You can set it to *, in which case it will find the first available port from 8080 and upward."},
			{"Raven/HttpCompression", "bool", "true,false", "Whatever http compression is enabled. Default: true."},
			{"Raven/VirtualDirectory", "string", null, "The virtual directory for the RavenDB server. Default: none."},

			// Access-Control headers
			{"Raven/AccessControlAllowOrigin", "string", null, "Configures the server to send Access-Control-Allow-Origin header with the specified value. Default: none. If this value isn't specified, all the access control settings are ignored."},
			{"Raven/AccessControlMaxAge", "int", null, "Configures the server to send Access-Control-Max-Age header with the specified value. Default: 1728000 (20 days)."},
			{"Raven/AccessControlAllowMethods", "string", null, "Configures the server to send Access-Control-Allow-Methods header with the specified value. Default: PUT,PATCH,GET,DELETE,POST."},
			{"Raven/AccessControlRequestHeaders", "string", null, "Configures the server to send Access-Control-Request-Headers header with the specified value. Default: none."},

			// Tenants
			{"Raven/Tenants/MaxIdleTimeForTenantDatabase", "int", null, "The time in seconds to allow a tenant database to be idle"},
			{"Raven/Tenants/FrequnecyToCheckForIdleDatabases", "int", null, "The time in seconds to check for an idle tenant database"},

			// Storage
			{"Raven/StorageTypeName", "string", "esent,munin,Fully Qualified Type Name", "The storage engine to use for the database. Default: esent."},

			// Indexing
			{"Raven/MaxNumberOfItemsToIndexInSingleBatch", "int", null, "The max number of items that will be indexed in a single batch. Larger batch size result in faster indexing, but higher memory usage."},
			{"Raven/InitialNumberOfItemsToIndexInSingleBatch", "int", null, "The number of items that will be indexed in a single batch. Larger batch size result in faster indexing, but higher memory usage."},
			{"Raven/AvailableMemoryForRaisingIndexBatchSizeLimit", "int", null, "The minimum amount of memory available for us to double the size of InitialNumberOfItemsToIndexInSingleBatch if we need to."},
			{"Raven/ResetIndexOnUncleanShutdown","bool", "false", "When the database is shut down rudely, determine whatever to reset the index or to check it. Note that checking the index may take some time on large databases."},
			{"Raven/MaxIndexingRunLatency", "TimeSpan", "00:01:00", "What is the suggested max latency for a single indexing run that allows the database to increase the indexing batch size"},
			

			{"Raven/TaskScheduler", "string", "assembly qualified type name", "The TaskScheduler type to use for executing indexing."},
			{"Raven/BackgroundTasksPriority", "string","Lowest,BelowNormal,Normal,AboveNormal,Highest", "The thread priority for indexing and other background tasks, (ignored if Raven/TaskScheduler is specified) default: Normal."},

			// Temp Indexing
			{"Raven/TempIndexPromotionMinimumQueryCount", "int", "1 or higher","The number of times a temporary index has to be queries during the promotion threshold to become a permanent auto index. Default: 100."},
			{"Raven/TempIndexPromotionThreshold", "int", null, "The promotion threshold for promoting a temporary dynamic index into a permanent auto index. The value is in second and refer to the length of time that the index have to get to the minimum query count value. Default: 10 minutes."},
			{"Raven/TempIndexCleanupPeriod","int", null, "How often will temp dynamic indexes be purged from the system. The value is in seconds. Default: 10 minutes."},
			{"Raven/TempIndexCleanupThreshold","int",null, "How long does a temporary index hang around if there are no queries made to it. The value is in seconds. Default: 20 minutes."},
			{"Raven/TempIndexInMemoryMaxMB", "int", "1 - 1024 MB", "The max size in MB of a temporary index held in memory. When a temporary dynamic index exceeds that value, it will be using on disk indexing, rather then RAM indexing. Default: 25 MB."},
			{"Raven/CreateTemporaryIndexesForAdHocQueriesIfNeeded", "bool", "true", "Whatever we allow creation of temporary indexes on dynamic queries"},
			// Memory
			{"Raven/MemoryCacheLimitMegabytes", "int", null, "The max size in MB for the internal document cache inside RavenDB server, default is half of the machine available RAM minus the size of the esent cache."},
			{"Raven/MemoryCacheLimitPercentage","int", "0-99", "The percentage of memory that the internal document cache inside RavenDB server will use, default: auto."},
			{"Raven/MemoryCacheLimitCheckInterval", "TimeSpan", "HH:MM:SS", "The internal for checking that the internal document cache inside RavenDB server will be cleaned, default: 2 minutes."},
			{"Raven/MemoryCacheExpiration", "int", null, "The expiration value for documents in the internal document cache. Value is in seconds. Default: 5 minutes"},
			// Esent
			{"Raven/Esent/CacheSizeMax", "int", null, "The size in MB of the Esent page cache, which is the default storage engine. Default: 25% of RAM on 64 bits, 256 MB on 32 bits."},
			{"Raven/Esent/MaxVerPages", "int", null, "The maximum size of version store (in memory modified data) available. The value is in megabytes. Default: 128."},
			{"Raven/Esent/LogFileSize", "int", null, "The size of the database log file. The value is in megabytes. Default: 64."},
			{"Raven/Esent/LogBuffers", "int", null, "The size of the in memory buffer for transaction log. Default: 16."},
			{"Raven/Esent/MaxCursors", "int", null, "The maximum number of cursors allowed concurrently. Default: 2048."},
			{"Raven/Esent/DbExtensionSize", "int", null, "The size that the database file will be enlarged with when the file is full. The value is in megabytes. Lower values result in smaller file size, but slower performance when the database size grows. Default: 8."},
			{"Raven/Esent/CircularLog", "bool", "true / false", "Whatever circular logs will be used, defaults to true. If you want to use incremental backups, you need to turn this off, but logs will only be trucated on backup."},

			// Advanced
			{"Raven/TransactionMode", "string", "lazy,safe", "What transaction mode to use. Safe transaction mode ensures data consistency, but is slower. Lazy is faster, but may result in a data loss if the server crashes. Default: Safe."},
			{"Raven/MaxNumberOfParallelIndexTasks", "int", "1 or higher", "The number of indexing tasks that can be run in parallel. There is usually one or two indexing tasks for each index. Default: machine processor count."},
			{"Raven/SkipCreatingStudioIndexes", "bool", "true,false", "Control whatever the Studio default indexes will be created or not. These default indexes are only used by the UI, and are not required for RavenDB to operate. Default: false"},
			{"Raven/LimitIndexesCapabilities","bool","true,false","Control whatever RavenDB limits what the indexes can do (to avoid potentially distabilizing operations)."},
			// Plugins
			{"Raven/PluginsDirectory", "string", null, "The location of the plugins directory for this database. Default: ~\\Plugins."},
			{"Raven/BundlesSearchPattern", "string", null, "Allow to limit the loaded plugins by specifying a search pattern, such as Raven.*.dll. Multiple values can be specified, separated by a semi column (;)."},
			{"Raven/ActiveBundles", "string", null, "Semi-column separated list of bundles names, such as: 'Replication;Versioning'. If the value is not specified, none of the bundles are installed."},

			// Obsolete
			{"Raven/WebDir", "string", null, "The location of the web directory for known files that makes up the RavenDB internal website. Default: Raven/WebUI"},

			// Bundles
			{"Raven/Quotas/Size/HardLimitInKB", "int", null, "The hard limit after which we refuse any additional writes. Default: none."},
			{"Raven/Quotas/Size/SoftMarginInKB", "int", null, "The soft limit before which we will warn about the quota. Default: 1 MB."},

			//Licensing
			{"Raven/License", "string", null, "The full license string for RavenDB. If Raven/License is specified, it overrides the Raven/LicensePath configuration."},
			{"Raven/LicensePath", "string", null, "The path to the license file for RavenDB, default for ~\\license.xml."},

		};

		private readonly List<ConfigOption> inner = new List<ConfigOption>();

		private ConfigOptionDocs()
		{
			
		}

		public IEnumerator<string> GetEnumerator()
		{
			foreach (var configOptionDoc in inner)
			{
				yield return string.Format("{0} {1} {2}\r\n{3}",
				                           configOptionDoc.Option,
										   configOptionDoc.Type, 
										   configOptionDoc.Range,
				                           configOptionDoc.Doc);
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public void Add(string option, string type, string range,  string doc)
		{
			inner.Add( new ConfigOption
			{
				Doc = doc,
				Option = option,
				Range = range,
				Type = type
			});
		}

		public class ConfigOption
		{
			public string Option { get;  set; }

			public string Type { get;  set; }

			public string Range { get;  set; }

			public string Doc { get;  set; }
		}

	}
}