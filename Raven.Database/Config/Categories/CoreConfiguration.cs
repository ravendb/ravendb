using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web;
using Raven.Abstractions.Data;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Util;
using Raven.Database.Plugins.Catalogs;
using Raven.Database.Server;
using Raven.Database.Util;

namespace Raven.Database.Config.Categories
{
    public class CoreConfiguration : ConfigurationCategory
    {
        private readonly InMemoryRavenConfiguration parent;
        internal static readonly int DefaultMaxNumberOfItemsToProcessInSingleBatch = Environment.Is64BitProcess ? 128 * 1024 : 16 * 1024;
        private readonly int defaultInitialNumberOfItemsToProcessInSingleBatch = Environment.Is64BitProcess ? 512 : 256;

        private int? maxNumberOfParallelIndexTasks;
        private bool runInMemory;
        private string workingDirectory;
        private string dataDirectory;
        private string indexStoragePath;
        private string pluginsDirectory;
        private string assembliesDirectory;
        private string embeddedFilesDirectory;
        private string compiledIndexCacheDirectory;
        private string virtualDirectory;

        public CoreConfiguration(InMemoryRavenConfiguration parent)
        {
            this.parent = parent;
            MaxNumberOfItemsToProcessInSingleBatch = DefaultMaxNumberOfItemsToProcessInSingleBatch;
            MaxNumberOfItemsToReduceInSingleBatch = DefaultMaxNumberOfItemsToProcessInSingleBatch / 2;
            MaxNumberOfParallelProcessingTasks = Environment.ProcessorCount;
            WebDir = GetDefaultWebDir();
            TempPath = Path.GetTempPath();
            VirtualDirectory = GetDefaultVirtualDirectory();
        }

        /// <summary>
        /// The maximum allowed page size for queries. 
        /// Default: 1024
        /// Minimum: 10
        /// </summary>
        [DefaultValue(1024)]
        [MinValue(10)]
        [ConfigurationEntry("Raven/MaxPageSize")]
        public int MaxPageSize { get; set; }

        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/MaxProcessingRunLatency")]
        [ConfigurationEntry("Raven/MaxIndexingRunLatency")]
        public TimeSetting MaxProcessingRunLatency { get; set; }

        /// <summary>
        /// Max number of items to take for indexing in a batch
        /// Minimum: 128
        /// </summary>
        [DefaultValue(DefaultValueSetInConstructor)]
        [MinValue(128)]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToProcessInSingleBatch")]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToIndexInSingleBatch")]
        public int MaxNumberOfItemsToProcessInSingleBatch { get; set; }

        /// <summary>
        /// Max number of items to take for reducing in a batch
        /// Minimum: 128
        /// </summary>
        [DefaultValue(DefaultValueSetInConstructor)]
        [MinValue(128)]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToReduceInSingleBatch")]
        public int MaxNumberOfItemsToReduceInSingleBatch { get; set; }

        /// <summary>
        /// The number that controls the if single step reduce optimization is performed.
        /// If the count of mapped results if less than this value then the reduce is executed in single step.
        /// Default: 1024
        /// </summary>
        [DefaultValue(1024)]
        [ConfigurationEntry("Raven/NumberOfItemsToExecuteReduceInSingleStep")]
        public int NumberOfItemsToExecuteReduceInSingleStep { get; set; }

        /// <summary>
        /// The maximum number of indexing, replication and sql replication tasks allowed to run in parallel
        /// Default: The number of processors in the current machine
        /// </summary>
        [DefaultValue(DefaultValueSetInConstructor)]
        [MinValue(1)]
        [ConfigurationEntry("Raven/MaxNumberOfParallelProcessingTasks")]
        [ConfigurationEntry("Raven/MaxNumberOfParallelIndexTasks")]
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
        /// Should RavenDB's storage be in-memory. If set to true, Voron would be used as the
        /// storage engine, regardless of what was specified for StorageTypeName
        /// Allowed values: true/false
        /// Default: false
        /// </summary>
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/RunInMemory")]
        public bool RunInMemory
        {
            get { return runInMemory; }
            set
            {
                runInMemory = value;
                parent.Settings[InMemoryRavenConfiguration.GetKey(x => x.Core.RunInMemory)] = value.ToString();
            }
        }

        [DefaultValue(@"~\")]
        [ConfigurationEntry("Raven/WorkingDir")]
        public string WorkingDirectory
        {
            get { return workingDirectory; }
            set { workingDirectory = CalculateWorkingDirectory(value); }
        }

        /// <summary>
        /// The directory for the RavenDB database. 
        /// You can use the ~\ prefix to refer to RavenDB's base directory. 
        /// Default: ~\Databases\System
        /// </summary>
        [DefaultValue(@"~\Databases\System")]
        [ConfigurationEntry("Raven/DataDir")]
        public string DataDirectory
        {
            get { return dataDirectory; }
            set { dataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(WorkingDirectory, value); }
        }

        [DefaultValue((string)null)]
        [ConfigurationEntry("Raven/IndexStoragePath")]
        public string IndexStoragePath
        {
            get
            {
                if (string.IsNullOrEmpty(indexStoragePath))
                    indexStoragePath = Path.Combine(DataDirectory, "Indexes");
                return indexStoragePath;
            }
            set
            {
                if (string.IsNullOrEmpty(value))
                    return;
                indexStoragePath = value.ToFullPath();
            }
        }

        /// <summary>
        /// The hostname to use when creating the http listener (null to accept any hostname or address)
        /// Default: none, binds to all host names
        /// </summary>
        [DefaultValue((string)null)]
        [ConfigurationEntry("Raven/HostName")]
        public string HostName { get; set; }

        /// <summary>
        /// The port to use when creating the http listener. 
        /// Default: 8080. You can set it to *, in which case it will find the first available port from 8080 and upward.
        /// </summary>
        [DefaultValue("*")]
        [ConfigurationEntry("Raven/Port")]
        public string PortStringValue { get; set; }

        public int Port { get; set; }

        /// <summary>
        /// Allow to get config information over the wire.
        /// Applies to endpoints: /debug/config, /debug...
        /// Default: Open. You can set it to AdminOnly.
        /// </summary>
        [DefaultValue("Open")]
        [ConfigurationEntry("Raven/ExposeConfigOverTheWire")]
        public string ExposeConfigOverTheWire { get; set; }

        /// <summary>
        /// The directory to search for RavenDB's WebUI. 
        /// This is usually only useful if you are debugging RavenDB's WebUI. 
        /// Default: ~/Raven/WebUI 
        /// </summary>
        [DefaultValue(DefaultValueSetInConstructor)]
        [ConfigurationEntry("Raven/WebDir")]
        public string WebDir { get; set; }

        /// <summary>
        /// Where to look for plugins for RavenDB. 
        /// Default: ~\Plugins
        /// </summary>
        [DefaultValue(@"~\Plugins")]
        [ConfigurationEntry("Raven/PluginsDirectory")]
        public string PluginsDirectory
        {
            get { return pluginsDirectory; }
            set
            {
                parent.ResetContainer();
                // remove old directory catalog
                var matchingCatalogs = parent.Catalog.Catalogs.OfType<DirectoryCatalog>()
                    .Concat(parent.Catalog.Catalogs.OfType<Plugins.Catalogs.FilteredCatalog>()
                        .Select(x => x.CatalogToFilter as DirectoryCatalog)
                        .Where(x => x != null)
                    )
                    .Where(c => c.Path == pluginsDirectory)
                    .ToArray();
                foreach (var cat in matchingCatalogs)
                {
                    parent.Catalog.Catalogs.Remove(cat);
                }

                pluginsDirectory = FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(WorkingDirectory, value);

                // add new one
                if (Directory.Exists(pluginsDirectory))
                {
                    var patterns = parent.Settings["Raven/BundlesSearchPattern"] ?? "*.dll";
                    foreach (var pattern in patterns.Split(new[] { ";" }, StringSplitOptions.RemoveEmptyEntries))
                    {
                        parent.Catalog.Catalogs.Add(new BuiltinFilteringCatalog(new DirectoryCatalog(pluginsDirectory, pattern)));
                    }
                }
            }
        }

        /// <summary>
        /// Where the internal assemblies will be extracted to.
        /// Default: ~\Assemblies
        /// </summary>
        [DefaultValue(@"~\Assemblies")]
        [ConfigurationEntry("Raven/AssembliesDirectory")]
        public string AssembliesDirectory
        {
            get
            {
                return assembliesDirectory;
            }
            set
            {
                assembliesDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(WorkingDirectory, value);
            }
        }

        /// <summary>
        /// Where we search for embedded files.
        /// Default: null
        /// </summary>
        [DefaultValue((string)null)]
        [ConfigurationEntry("Raven/EmbeddedFilesDirectory")]
        public string EmbeddedFilesDirectory
        {
            get { return embeddedFilesDirectory; }
            set { embeddedFilesDirectory = value.ToFullPath(); }
        }

        /// <summary>
        /// Where to cache the compiled indexes. Absolute path or relative to TEMP directory.
        /// Default: ~\CompiledIndexCache
        /// </summary>
        [DefaultValue(@"~\CompiledIndexCache")]
        [ConfigurationEntry("Raven/CompiledIndexCacheDirectory")]
        public string CompiledIndexCacheDirectory
        {
            get
            {
                return compiledIndexCacheDirectory;
            }
            set
            {
                compiledIndexCacheDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(WorkingDirectory, value);
            }
        }

        [DefaultValue((string)null)]
        [ConfigurationEntry("Raven/TaskScheduler")]
        public string TaskScheduler { get; set; }

        /// <summary>
        /// The initial number of items to take when processing a batch
        /// Default: 512 or 256 depending on CPU architecture
        /// </summary>
        public int InitialNumberOfItemsToProcessInSingleBatch { get; set; }

        /// <summary>
        /// The initial number of items to take when reducing a batch
        /// Default: 256 or 128 depending on CPU architecture
        /// </summary>
        public int InitialNumberOfItemsToReduceInSingleBatch { get; set; }

        /// <summary>
        /// If set all client request to the server will be rejected with 
        /// the http 503 response.
        /// Other servers or the studio could still access the server.
        /// </summary>
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/RejectClientsModeEnabled")]
        public bool RejectClientsMode { get; set; }

        /// <summary>
        /// The time to wait before canceling a database operation such as load (many) or query
        /// </summary>
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/DatabaseOperationTimeoutInMin")]
        [ConfigurationEntry("Raven/DatabaseOperationTimeout")]
        public TimeSetting DatabaseOperationTimeout { get; set; }

        /// <summary>
        /// If True, turns off the discovery client.
        /// </summary>
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/TurnOffDiscoveryClient")]
        public bool TurnOffDiscoveryClient { get; set; }

        /// <summary>
        /// The maximum number of recent document touches to store (i.e. updates done in
        /// order to initiate indexing rather than because something has actually changed).
        /// </summary>
        [DefaultValue(1024)]
        [ConfigurationEntry("Raven/MaxRecentTouchesToRemember")]
        public int MaxRecentTouchesToRemember { get; set; }


        /// <summary>
        /// Determines how long replication and periodic backup tombstones will be kept by a database. After the specified time they will be automatically
        /// purged on next database startup. Default: 14 days.
        /// </summary>
        [DefaultValue(14)]
        [TimeUnit(TimeUnit.Days)]
        [ConfigurationEntry("Raven/TombstoneRetentionTimeInDays")]
        [ConfigurationEntry("Raven/TombstoneRetentionTime")]
        public TimeSetting TombstoneRetentionTime { get; set; }

        /// <summary>
        /// How FieldsToFetch are extracted from the document.
        /// Default: Enabled. 
        /// Other values are: 
        ///     DoNothing (fields are not fetched from the document)
        ///     Exception (an exception is thrown if we need to fetch fields from the document itself)
        /// </summary>
        [DefaultValue(ImplicitFetchFieldsMode.Enabled)]
        [ConfigurationEntry("Raven/ImplicitFetchFieldsFromDocumentMode")]
        public ImplicitFetchFieldsMode ImplicitFetchFieldsFromDocumentMode { get; set; }

        /// <summary>
        /// Path to temporary directory used by server.
        /// Default: Current user's temporary directory
        /// </summary>
        [DefaultValue(DefaultValueSetInConstructor)]
        [ConfigurationEntry("Raven/TempPath")]
        public string TempPath { get; set; }

        /// <summary>
        /// What sort of transaction mode to use. 
        /// Allowed values: 
        /// Lazy - faster, but can result in data loss in the case of server crash. 
        /// Safe - slower, but will never lose data 
        /// Default: Safe 
        /// </summary>
        [DefaultValue(TransactionMode.Safe)]
        [ConfigurationEntry("Raven/TransactionMode")]
        public TransactionMode TransactionMode { get; set; }

        /// <summary>
        /// Defines which operations are allowed for anonymous users.
        /// Allowed values: All, Get, None
        /// Default: Get
        /// </summary>
        [DefaultValue(AnonymousUserAccessMode.Admin)]
        [ConfigurationEntry("Raven/AnonymousAccess")]
        public AnonymousUserAccessMode AnonymousUserAccessMode { get; set; }

        /// <summary>
        /// The virtual directory to use when creating the http listener. 
        /// Default: / 
        /// </summary>
        [DefaultValue(DefaultValueSetInConstructor)] // set in initialize
        [ConfigurationEntry("Raven/VirtualDirectory")]
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

        [DefaultValue(IgnoreSslCertificateErrorsMode.None)]
        [ConfigurationEntry("Raven/IgnoreSslCertificateErrors")]
        public IgnoreSslCertificateErrorsMode IgnoreSslCertificateErrors { get; set; }

        [DefaultValue("")]
        [ConfigurationEntry("Raven/Headers/Ignore")]
        public string HeadersToIgnoreStringValue { get; set; }

        public HashSet<string> HeadersToIgnore { get; private set; }

        public override void Initialize(NameValueCollection settings)
        {
            base.Initialize(settings);

            var initialNumberOfItemsToIndexInSingleBatch = settings["Raven/InitialNumberOfItemsToProcessInSingleBatch"] ?? settings["Raven/InitialNumberOfItemsToIndexInSingleBatch"];
            if (initialNumberOfItemsToIndexInSingleBatch != null)
            {
                InitialNumberOfItemsToProcessInSingleBatch = Math.Min(int.Parse(initialNumberOfItemsToIndexInSingleBatch), MaxNumberOfItemsToProcessInSingleBatch);
            }
            else
            {
                InitialNumberOfItemsToProcessInSingleBatch = MaxNumberOfItemsToProcessInSingleBatch == (int)GetDefaultValue<CoreConfiguration>(x => x.MaxNumberOfItemsToProcessInSingleBatch) ?
                    defaultInitialNumberOfItemsToProcessInSingleBatch :
                    Math.Max(16, Math.Min(MaxNumberOfItemsToProcessInSingleBatch / 256, defaultInitialNumberOfItemsToProcessInSingleBatch));
            }

            var initialNumberOfItemsToReduceInSingleBatch = settings["Raven/InitialNumberOfItemsToReduceInSingleBatch"];
            if (initialNumberOfItemsToReduceInSingleBatch != null)
            {
                InitialNumberOfItemsToReduceInSingleBatch = Math.Min(int.Parse(initialNumberOfItemsToReduceInSingleBatch),
                    MaxNumberOfItemsToReduceInSingleBatch);
            }
            else
            {
                InitialNumberOfItemsToReduceInSingleBatch = MaxNumberOfItemsToReduceInSingleBatch == (int)GetDefaultValue<CoreConfiguration>(x => x.MaxNumberOfItemsToReduceInSingleBatch) ?
                    defaultInitialNumberOfItemsToProcessInSingleBatch / 2 :
                    Math.Max(16, Math.Min(MaxNumberOfItemsToReduceInSingleBatch / 256, defaultInitialNumberOfItemsToProcessInSingleBatch / 2));
            }

            if (string.IsNullOrEmpty(parent.DatabaseName)) // we only use this for root database
            {
                Port = PortUtil.GetPort(PortStringValue, RunInMemory);
            }

            if (string.IsNullOrEmpty(TaskScheduler) == false)
            {
                var type = Type.GetType(TaskScheduler);
                parent.CustomTaskScheduler = (TaskScheduler)Activator.CreateInstance(type);
            }

            HeadersToIgnore = new HashSet<string>(HeadersToIgnoreStringValue.GetSemicolonSeparatedValues(), StringComparer.OrdinalIgnoreCase);
        }

        private string GetDefaultVirtualDirectory()
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

            return defaultVirtualDirectory;
        }

        private string GetDefaultWebDir()
        {
            return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"Raven/WebUI");
        }

        private static string CalculateWorkingDirectory(string workingDirectory)
        {
            if (string.IsNullOrEmpty(workingDirectory))
                workingDirectory = @"~\";

            if (workingDirectory.StartsWith("APPDRIVE:", StringComparison.OrdinalIgnoreCase))
            {
                var baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
                var rootPath = Path.GetPathRoot(baseDirectory);
                if (string.IsNullOrEmpty(rootPath) == false)
                    workingDirectory = Regex.Replace(workingDirectory, "APPDRIVE:", rootPath.TrimEnd('\\'), RegexOptions.IgnoreCase);
            }

            return FilePathTools.MakeSureEndsWithSlash(workingDirectory.ToFullPath());
        }
    }
}