using System.Collections;
using System.Collections.Generic;

namespace Raven.Database.Config
{
    internal class ConfigOptionDocs : IEnumerable<string>
    {
        public readonly static ConfigOptionDocs OptionsDocs = new ConfigOptionDocs
        {
            // Common
            {"Raven/MaxPageSize", "int", null, "The maximum page size that can be specified on this server, default: 1,024."},
            {"Raven/RunInMemory", "bool", "true,false", "Whatever the database should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing. Default: false."},
            {"Raven/Headers/Ignore", "string", null, "Semicolon separated list of headers that server should ignore. e.g. Header-To-Ignore-1;Header-To-Ignore-2"},
            
            // Studio
            {"Raven/RedirectStudioUrl", "string", null, "The url to redirect the user to when then try to access the local studio"},

            // Paths
            {"Raven/DataDir", "string", null,"The path for the database directory. Can use ~\\ as the root, in which case the path will start from the server base directory. Default: ~\\Data."},
            {"Raven/IndexStoragePath", "string", null,"The path for the indexes on disk. Useful if you want to store the indexes on another HDD for performance reasons. Default: ~\\Data\\Indexes."},
            {"Raven/Esent/LogsPath", "string", null,"The path for the esent logs. Useful if you want to store the indexes on another HDD for performance reasons. Default: ~\\Data\\Logs."},

            // Authentication
            {"Raven/AnonymousAccess", "string", "Get,All,None", "Determines what actions an anonymous user can do. Get - read only, All - read & write, None - allows access to only authenticated users. Default: Get."},
            {"Raven/OAuthTokenServer", "string", null, "The url clients should use for authenticating when using OAuth mode. Default: http://RavenDB-Server-Url/OAuth/API-Key - the internal OAuth server"},
            {"Raven/OAuthTokenCertificate", "string", null, "The base 64 to the OAuth key use to communicate with the server. Default: none. If no key is specified, one will be automatically created."},

            // Network
            {"Raven/HostName", "string", null, "The hostname to bind the embedded http server to, if we want to bind to a specific hostname, rather than all. Default: none."},
            {"Raven/Port", "int", "1 - 65,536, *", "The port to bind the embedded http server. Default: 8080. You can set it to *, in which case it will find the first available port from 8080 and upward."},
            {"Raven/ExposeConfigOverTheWire", "string", null, "Allow to get config information over the wire. Default: Open."},
            {"Raven/UseSsl", "bool", "false", "Enable/disable SSL. Default: false."},
            {"Raven/HttpCompression", "bool", "true,false", "Whatever http compression is enabled. Default: true."},
            {"Raven/VirtualDirectory", "string", null, "The virtual directory for the RavenDB server. Default: none."},

            // Access-Control headers
            {"Raven/AccessControlAllowOrigin", "string", null, "Configures the server to send Access-Control-Allow-Origin header with the specified value. Default: none. If this value isn't specified, all the access control settings are ignored."},
            {"Raven/AccessControlMaxAge", "int", null, "Configures the server to send Access-Control-Max-Age header with the specified value. Default: 1728000 (20 days)."},
            {"Raven/AccessControlAllowMethods", "string", null, "Configures the server to send Access-Control-Allow-Methods header with the specified value. Default: PUT,PATCH,GET,DELETE,POST."},
            {"Raven/AccessControlRequestHeaders", "string", null, "Configures the server to send Access-Control-Request-Headers header with the specified value. Default: none."},

            // Tenants
            {"Raven/Tenants/MaxIdleTimeForTenantDatabase", "int", null, "The time in seconds to allow a tenant database to be idle"},
            {"Raven/Tenants/FrequencyToCheckForIdleDatabases", "int", null, "The time in seconds to check for an idle tenant database"},

            // Storage
            {"Raven/StorageTypeName", "string", "esent, voron or Assembly Qualified Name", "The storage engine to use for the database. Default: esent."},

            // Indexing
            {"Raven/MaxNumberOfItemsToIndexInSingleBatch", "int", null, "The max number of items that will be indexed in a single batch. Larger batch size result in faster indexing, but higher memory usage."},
            {"Raven/InitialNumberOfItemsToIndexInSingleBatch", "int", null, "The number of items that will be indexed in a single batch. Larger batch size result in faster indexing, but higher memory usage."},
            {"Raven/AvailableMemoryForRaisingIndexBatchSizeLimit", "int", null, "The minimum amount of memory available for us to double the size of InitialNumberOfItemsToIndexInSingleBatch if we need to."},
            {"Raven/ResetIndexOnUncleanShutdown","bool", "false", "When the database is shut down rudely, determine whatever to reset the index or to check it. Note that checking the index may take some time on large databases."},
            {"Raven/MaxIndexingRunLatency", "TimeSpan", "00:05:00", "What is the suggested max latency for a single indexing run that allows the database to increase the indexing batch size"},
            {"Raven/DisableDocumentPreFetchingForIndexing", "bool", "false","Disable document prefetching for indexes"},
            {"Raven/NumberOfItemsToExecuteReduceInSingleStep", "int", "10 - 100,000", "The number of items that will cause RavenDB to move to multi step reduce, default: 1,024"},
            {"Raven/TaskScheduler", "string", "assembly qualified type name", "The TaskScheduler type to use for executing indexing."},
            {"Raven/NewIndexInMemoryMaxMB", "int", "1 - 1024 MB", "The max size in MB of a new index held in memory. When a new index size reaches that value or is no longer stale, it will be using on disk indexing, rather then RAM indexing. Default: 64 MB."},
            {"Raven/Indexing/FlushIndexToDiskSizeInMb", "int", null, "Number of megabytes after which indexes are flushed to a disk. Default: 5"},

            // Encryption
            {"Raven/Encryption/FIPS", "bool", null, "Use FIPS compliant encryption algorithms. Default: false."},

            // Replication
            {"Raven/Replication/FetchingFromDiskTimeout", "int", null, "Number of seconds after which replication will stop reading documents/attachments from disk. Default: 30."},
            {"Raven/Replication/ReplicationRequestTimeout", "int", null, "Number of milliseconds before replication requests will timeout. Default: 60 * 1000."},
            
            // Prefetcher
            {"Raven/Prefetcher/FetchingDocumentsFromDiskTimeout", "int", null, "Number of seconds after which prefetcher will stop reading documents from disk. Default: 5."},
            {"Raven/Prefetcher/MaximumSizeAllowedToFetchFromStorage", "int", null, "Maximum number of megabytes after which prefetcher will stop reading documents from disk. Default: 256."},

            // Idle 
            {"Raven/TimeToWaitBeforeRunningIdleIndexes", "TimeSpan", "00:10:00", "How long the database should be idle for before updating low priority indexes, default: 10 minutes"},
            {"Raven/TimeToWaitBeforeMarkingAutoIndexAsIdle", "TimeSpan", "1:00:00", "How long the database should wait before marking an index with the idle flag, default: 1 hour"},

            {"Raven/TimeToWaitBeforeRunningAbandonedIndexes", "TimeSpan", "3:00:00", "How long the database should be idle for before updating abandoned indexes, default: 3 hours"},
            {"Raven/TimeToWaitBeforeMarkingIdleIndexAsAbandoned", "TimeSpan", "72:00:00", "How long the database should wait before marking an index with the abandoned flag, default: 72 hours"},


            // Auto Indexing
            {"Raven/CreateAutoIndexesForAdHocQueriesIfNeeded", "bool", "true", "Whatever we allow creation of auto indexes on dynamic queries"},
            
            // Memory
            {"Raven/MemoryCacheLimitMegabytes", "int", null, "The max size in MB for the internal document cache inside RavenDB server, default is half of the machine available RAM minus the size of the esent cache."},
            {"Raven/MemoryCacheLimitPercentage","int", "0-99", "The percentage of memory that the internal document cache inside RavenDB server will use, default: auto."},
            {"Raven/MemoryCacheLimitCheckInterval", "TimeSpan", "HH:MM:SS", "The internal for checking that the internal document cache inside RavenDB server will be cleaned."},
            {"Raven/MemoryCacheExpiration", "int", null, "The expiration value for documents in the internal document cache. Value is in seconds. Default: 60 minutes"},
            {"Raven/MemoryLimitForProcessing", "int", null, "Maximum number of megabytes that can be used by database to control the maximum size of the processing batches. Default: 1024 or 75% percent of available memory if 1GB is not available."},
            {"Raven/MemoryCacher", "string", "assembly qualified type name", "The MemoryCacher type to use for caching database documents."},

            // Esent
            {"Raven/Esent/CacheSizeMax", "int", null, "The size in MB of the Esent page cache, which is the default storage engine. Default: 25% of RAM on 64 bits, 256 MB on 32 bits."},
            {"Raven/Esent/MaxVerPages", "int", null, "The maximum size of version store (in memory modified data) available. The value is in megabytes. Default: 512."},
            {"Raven/Esent/PreferredVerPages", "int", null, "The preferred size of version store (in memory modified data) available. If the value exceed that level, optional background tasks data are removed from the version store. The value is in megabytes. Default: 472."},
            {"Raven/Esent/LogFileSize", "int", null, "The size of the database log file. The value is in megabytes. Default: 64."},
            {"Raven/Esent/LogBuffers", "int", null, "The size of the in memory buffer for transaction log. Default: 8192."},
            {"Raven/Esent/MaxCursors", "int", null, "The maximum number of cursors allowed concurrently. Default: 2048."},
            {"Raven/Esent/DbExtensionSize", "int", null, "The size that the database file will be enlarged with when the file is full. The value is in megabytes. Lower values result in smaller file size, but slower performance when the database size grows. Default: 8."},
            {"Raven/Esent/CircularLog", "bool", "true / false", "Whatever circular logs will be used, defaults to true. If you want to use incremental backups, you need to turn this off, but logs will only be truncated on backup."},

            //Voron	
            {"Raven/Voron/AllowIncrementalBackups", "bool", "true / false", "If you want to use incremental backups, you need to turn this to true, but then journal files will not be deleted after applying them to the data file. They will be deleted only after a successful backup. Default: false."},
            {"Raven/Voron/TempPath", "string", null, "You can use this setting to specify a different path to temporary files. By default it is empty, which means that temporary files will be created at same location as data file."},
            {"Raven/Voron/MaxBufferPoolSize", "long", null, "You can use this setting to specify a maximum buffer pool size that can be used for transactional storage (in gigabytes). By default it is 4. Minimum value is 2."},
            {"Raven/Voron/InitialSize", "long", null, "You can use this setting to specify an initial file size for data file (in bytes)."},
            {"Raven/Voron/MaxScratchBufferSize", "int", null, "The maximum scratch buffer (modified data by active transactions) size that can be used by Voron. The value is in megabytes. Default: 1024."},

            // Advanced
            {"Raven/TransactionMode", "string", "lazy,safe", "What transaction mode to use. Safe transaction mode ensures data consistency, but is slower. Lazy is faster, but may result in a data loss if the server crashes. Default: Safe."},
            {"Raven/MaxNumberOfParallelIndexTasks", "int", "1 or higher", "The number of indexing tasks that can be run in parallel. There is usually one or two indexing tasks for each index. Default: machine processor count."},
            {"Raven/SkipCreatingStudioIndexes", "bool", "true,false", "Control whatever the Studio default indexes will be created or not. These default indexes are only used by the UI, and are not required for RavenDB to operate. Default: false"},
            {"Raven/LimitIndexesCapabilities","bool","true,false","Control whatever RavenDB limits what the indexes can do (to avoid potentially destabilizing operations)."},
            // Plugins
            {"Raven/PluginsDirectory", "string", null, "The location of the plugins directory for this database. Default: ~\\Plugins."},
            {"Raven/BundlesSearchPattern", "string", null, "Allow to limit the loaded plugins by specifying a search pattern, such as Raven.*.dll. Multiple values can be specified, separated by a semi column (;)."},
            {"Raven/ActiveBundles", "string", null, "Semicolon separated list of bundles names, such as: 'Replication;Versioning'. If the value is not specified, none of the bundles are installed."},

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

        private void Add(string option, string type, string range,  string doc)
        {
            inner.Add( new ConfigOption
            {
                Doc = doc,
                Option = option,
                Range = range,
                Type = type
            });
        }

        internal class ConfigOption
        {
            public string Option { get;  set; }

            public string Type { get;  set; }

            public string Range { get;  set; }

            public string Doc { get;  set; }
        }

    }
}
