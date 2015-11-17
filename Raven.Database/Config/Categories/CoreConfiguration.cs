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
        private readonly RavenConfiguration parent;
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

        public CoreConfiguration(RavenConfiguration parent)
        {
            this.parent = parent;
            MaxNumberOfItemsToProcessInSingleBatch = DefaultMaxNumberOfItemsToProcessInSingleBatch;
            MaxNumberOfItemsToReduceInSingleBatch = DefaultMaxNumberOfItemsToProcessInSingleBatch / 2;
            MaxNumberOfParallelProcessingTasks = Environment.ProcessorCount;
            WebDir = GetDefaultWebDir();
            TempPath = Path.GetTempPath();
            VirtualDirectory = GetDefaultVirtualDirectory();
        }

        [Description("The maximum allowed page size for queries")]
        [DefaultValue(1024)]
        [MinValue(10)]
        [ConfigurationEntry("Raven/MaxPageSize")]
        public int MaxPageSize { get; set; }

        [Description("What is the suggested max latency for a single processing run that allows the database to increase the processing batch size")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/MaxProcessingRunLatency")]
        [ConfigurationEntry("Raven/MaxIndexingRunLatency")]
        public TimeSetting MaxProcessingRunLatency { get; set; }

        [Description("The max number of items that will be processed in a single batch. Larger batch size result in faster processing, but higher memory usage.")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [MinValue(128)]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToProcessInSingleBatch")]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToIndexInSingleBatch")]
        public int MaxNumberOfItemsToProcessInSingleBatch { get; set; }

        [Description("Max number of items to take for reducing in a batch")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [MinValue(128)]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToReduceInSingleBatch")]
        public int MaxNumberOfItemsToReduceInSingleBatch { get; set; }

        [Description("The number that controls the if single step reduce optimization is performed." +
                     "If the count of mapped results if less than this value then the reduce is executed in single step.")]
        [DefaultValue(1024)]
        [ConfigurationEntry("Raven/NumberOfItemsToExecuteReduceInSingleStep")]
        public int NumberOfItemsToExecuteReduceInSingleStep { get; set; }

        [Description("The maximum number of indexing, replication and sql replication tasks allowed to run in parallel.\r\nDefault: The number of processors in the current machine")]
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

        [Description("Whatever the database should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost. This is mostly useful for testing.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/RunInMemory")]
        public bool RunInMemory
        {
            get { return runInMemory; }
            set
            {
                runInMemory = value;
            }
        }

        [DefaultValue(@"~\")]
        [ConfigurationEntry("Raven/WorkingDir")]
        public string WorkingDirectory
        {
            get { return workingDirectory; }
            set { workingDirectory = CalculateWorkingDirectory(value); }
        }

        [Description("The directory for the RavenDB database. You can use the ~\\ prefix to refer to RavenDB's base directory.")]
        [DefaultValue(@"~\Databases\System")]
        [ConfigurationEntry("Raven/DataDir")]
        public string DataDirectory
        {
            get { return dataDirectory; }
            set { dataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(WorkingDirectory, value); }
        }

        [Description("The path for the indexes on disk. Useful if you want to store the indexes on another HDD for performance reasons.\r\nDefault: ~\\Databases\\[database-name]\\Indexes.")]
        [DefaultValue(null)]
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

        [Description("The hostname to use when creating the http listener (null to accept any hostname or address). \r\nDefault: none, binds to all host names")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/HostName")]
        public string HostName { get; set; }

        [Description("The port to use when creating the http listener.\r\nDefault: 8080. You can set it to *, in which case it will find the first available port from 8080 and upward.")]
        [DefaultValue("*")]
        [ConfigurationEntry("Raven/Port")]
        public string PortStringValue { get; set; }

        public int Port { get; set; }

        [Description("Allow to get config information over the wire. Applies to endpoints: /debug/config, /debug...")]
        [DefaultValue("Open")]
        [ConfigurationEntry("Raven/ExposeConfigOverTheWire")]
        public string ExposeConfigOverTheWire { get; set; }

        [Description("The directory to search for RavenDB's WebUI. This is usually only useful if you are debugging RavenDB's WebUI. \r\nDefault: ~/Raven/WebUI")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [ConfigurationEntry("Raven/WebDir")]
        public string WebDir { get; set; }

        [Description("Allow to limit the loaded plugins by specifying a search pattern, such as Raven.*.dll. Multiple values can be specified, separated by a semi column (;).")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/BundlesSearchPattern")]
        public string BundlesSearchPattern { get; set; }

        [Description("The location of the plugins directory for RavenDB")]
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
                    var patterns = BundlesSearchPattern ?? "*.dll";
                    foreach (var pattern in patterns.Split(new[] { ";" }, StringSplitOptions.RemoveEmptyEntries))
                    {
                        parent.Catalog.Catalogs.Add(new BuiltinFilteringCatalog(new DirectoryCatalog(pluginsDirectory, pattern)));
                    }
                }
            }
        }

        [Description("Where the internal assemblies will be extracted to")]
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

        [Description("Where we search for embedded files")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/EmbeddedFilesDirectory")]
        public string EmbeddedFilesDirectory
        {
            get { return embeddedFilesDirectory; }
            set { embeddedFilesDirectory = value.ToFullPath(); }
        }

        [Description("Where to cache the compiled indexes. Absolute path or relative to TEMP directory.")]
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

        [Description("The TaskScheduler type to use for executing indexing. Provided as assembly qualified type name.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/TaskScheduler")]
        public string TaskScheduler { get; set; }
        
        [Description("The number of items that will be processed in a single batch. Larger batch size result in faster processing, but higher memory usage.\r\nDefault: 512 or 256 depending on CPU architecture")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/InitialNumberOfItemsToProcessInSingleBatch")]
        [ConfigurationEntry("Raven/InitialNumberOfItemsToIndexInSingleBatch")]
        // ReSharper disable once InconsistentNaming
        public int? _InitialNumberOfItemsToProcessInSingleBatch { get; set; }

        public int InitialNumberOfItemsToProcessInSingleBatch { get; set; }

        [Description("The initial number of items to take when reducing a batch.\r\nDefault: 256 or 128 depending on CPU architecture")]
        [DefaultValue(null)]
        [ConfigurationEntry("Raven/InitialNumberOfItemsToReduceInSingleBatch")]
        // ReSharper disable once InconsistentNaming
        public int? _InitialNumberOfItemsToReduceInSingleBatch { get; set; }

        public int InitialNumberOfItemsToReduceInSingleBatch { get; set; }

        [Description("If set all client request to the server will be rejected with the http 503 response. Other servers or the studio could still access the server.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/RejectClientsModeEnabled")]
        public bool RejectClientsMode { get; set; }

        [Description("The time to wait before canceling a database operation such as load (many) or query")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Raven/DatabaseOperationTimeoutInMin")]
        [ConfigurationEntry("Raven/DatabaseOperationTimeout")]
        public TimeSetting DatabaseOperationTimeout { get; set; }

        [Description("If True, turns off the discovery client")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/TurnOffDiscoveryClient")]
        public bool TurnOffDiscoveryClient { get; set; }

        [Description("The maximum number of recent document touches to store (i.e. updates done in order to initiate indexing rather than because something has actually changed).")]
        [DefaultValue(1024)]
        [ConfigurationEntry("Raven/MaxRecentTouchesToRemember")]
        public int MaxRecentTouchesToRemember { get; set; }


        [Description("Determines how long replication and periodic backup tombstones will be kept by a database. After the specified time they will be automatically purged on next database startup. Default: 14 days.")]
        [DefaultValue(14)]
        [TimeUnit(TimeUnit.Days)]
        [ConfigurationEntry("Raven/TombstoneRetentionTimeInDays")]
        [ConfigurationEntry("Raven/TombstoneRetentionTime")]
        public TimeSetting TombstoneRetentionTime { get; set; }

        [Description("How FieldsToFetch are extracted from the document. Other values are:\r\n"+
            "\tDoNothing (fields are not fetched from the document)\r\n" +
            "\tException (an exception is thrown if we need to fetch fields from the document itself)")]
        [DefaultValue(ImplicitFetchFieldsMode.Enabled)]
        [ConfigurationEntry("Raven/ImplicitFetchFieldsFromDocumentMode")]
        public ImplicitFetchFieldsMode ImplicitFetchFieldsFromDocumentMode { get; set; }

        [Description("Path to temporary directory used by server\r\nDefault: Current user's temporary directory")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [ConfigurationEntry("Raven/TempPath")]
        public string TempPath { get; set; }

        [Description("What sort of transaction mode to use. Allowed values: \r\n" +
                     "\tLazy - faster, but can result in data loss in the case of server crash\r\n" +
                     "\tSafe - slower, but will never lose data")]
        [DefaultValue(TransactionMode.Safe)]
        [ConfigurationEntry("Raven/TransactionMode")]
        public TransactionMode TransactionMode { get; set; }

        [Description("Defines which operations are allowed for anonymous users. Allowed values: Get - read only, All - read & write, None - allows access to only authenticated users")]
        [DefaultValue(AnonymousUserAccessMode.Admin)]
        [ConfigurationEntry("Raven/AnonymousAccess")]
        public AnonymousUserAccessMode AnonymousUserAccessMode { get; set; }

        [Description("The virtual directory to use when creating the http listener.\r\nDefault: / ")]
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

        [Description("Semicolon separated list of headers that server should ignore. e.g. Header-To-Ignore-1;Header-To-Ignore-2")]
        [DefaultValue("")]
        [ConfigurationEntry("Raven/Headers/Ignore")]
        // ReSharper disable once InconsistentNaming
        public string _HeadersToIgnoreString { get; set; }

        [Description("Semicolon separated list of bundles names, such as: 'Replication;Versioning'. If the value is not specified, none of the bundles are installed.")]
        [DefaultValue("")]
        [ConfigurationEntry("Raven/ActiveBundles")]
        public string _ActiveBundlesString { get; set; }

        public IEnumerable<string> ActiveBundles => BundlesHelper.ProcessActiveBundles(_ActiveBundlesString)
            .GetSemicolonSeparatedValues()
            .Distinct();

        public HashSet<string> HeadersToIgnore { get; private set; }

        public override void Initialize(NameValueCollection settings)
        {
            base.Initialize(settings);
            
            if (_InitialNumberOfItemsToProcessInSingleBatch != null)
            {
                InitialNumberOfItemsToProcessInSingleBatch = Math.Min(_InitialNumberOfItemsToProcessInSingleBatch.Value, MaxNumberOfItemsToProcessInSingleBatch);
            }
            else
            {
                InitialNumberOfItemsToProcessInSingleBatch = MaxNumberOfItemsToProcessInSingleBatch == (int)GetDefaultValue<CoreConfiguration>(x => x.MaxNumberOfItemsToProcessInSingleBatch) ?
                    defaultInitialNumberOfItemsToProcessInSingleBatch :
                    Math.Max(16, Math.Min(MaxNumberOfItemsToProcessInSingleBatch / 256, defaultInitialNumberOfItemsToProcessInSingleBatch));
            }
            
            if (_InitialNumberOfItemsToReduceInSingleBatch != null)
            {
                InitialNumberOfItemsToReduceInSingleBatch = Math.Min(_InitialNumberOfItemsToReduceInSingleBatch.Value, MaxNumberOfItemsToReduceInSingleBatch);
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

            HeadersToIgnore = new HashSet<string>(_HeadersToIgnoreString.GetSemicolonSeparatedValues(), StringComparer.OrdinalIgnoreCase);
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