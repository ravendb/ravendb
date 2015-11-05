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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using System.Runtime.Caching;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Rachis;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Plugins.Catalogs;
using Raven.Database.Server;
using Raven.Database.FileSystem.Util;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Extensions;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config
{
    public class InMemoryRavenConfiguration
    {
        private CompositionContainer container;
        private bool containerExternallySet;

        public CoreConfiguration Core { get; }

        public ReplicationConfiguration Replication { get; }

        public PrefetcherConfiguration Prefetcher { get; }

        public StorageConfiguration Storage { get; }

        public FileSystemConfiguration FileSystem { get; }

        public CounterConfiguration Counter { get; }
        
        public TimeSeriesConfiguration TimeSeries { get; }

        public EncryptionConfiguration Encryption { get; }

        public IndexingConfiguration Indexing { get; set; }

        public ClusterConfiguration Cluster { get; }

        public MonitoringConfiguration Monitoring { get; }

        public WebSocketsConfiguration WebSockets { get; set; }

        public QueryConfiguration Queries { get; }

        public PatchingConfiguration Patching { get;  }

        public BulkInsertConfiguration BulkInsert { get; }

        public ServerConfiguration Server { get; }

        public MemoryConfiguration Memory { get; }

        public OAuthConfiguration OAuth { get; private set; }

        public ExpirationBundleConfiguration Expiration { get; }

        public InMemoryRavenConfiguration()
        {
            Settings = new NameValueCollection(StringComparer.OrdinalIgnoreCase);

            Core = new CoreConfiguration(this);

            FileSystem = new FileSystemConfiguration(Core);
            Counter = new CounterConfiguration(Core);
            TimeSeries = new TimeSeriesConfiguration(Core);

            Replication = new ReplicationConfiguration();
            Prefetcher = new PrefetcherConfiguration();
            Storage = new StorageConfiguration();
            Encryption = new EncryptionConfiguration();
            Indexing = new IndexingConfiguration();
            WebSockets = new WebSocketsConfiguration();
            Cluster = new ClusterConfiguration();
            Monitoring = new MonitoringConfiguration();
            Queries = new QueryConfiguration();
            Patching = new PatchingConfiguration();
            BulkInsert = new BulkInsertConfiguration();
            Server = new ServerConfiguration();
            Memory = new MemoryConfiguration();
            Expiration = new ExpirationBundleConfiguration();

            IndexingClassifier = new DefaultIndexingClassifier();

            Catalog = new AggregateCatalog(CurrentAssemblyCatalog);

            Catalog.Changed += (sender, args) => ResetContainer();
        }

        public string DatabaseName { get; set; }

        public string FileSystemName { get; set; }

        public string CounterStorageName { get; set; }

        public string TimeSeriesName { get; set; }

        public void PostInit()
        {
            CheckDirectoryPermissions();

            FilterActiveBundles();

            OAuth = new OAuthConfiguration(ServerUrl);
            OAuth.Initialize(Settings);
        }

        public InMemoryRavenConfiguration Initialize()
        {
            Core.Initialize(Settings);
            Replication.Initialize(Settings);
            Queries.Initialize(Settings);
            Patching.Initialize(Settings);
            BulkInsert.Initialize(Settings);
            Server.Initialize(Settings);
            Memory.Initialize(Settings);
            Indexing.Initialize(Settings);
            Prefetcher.Initialize(Settings);
            Storage.Initialize(Settings);
            Encryption.Initialize(Settings);
            Cluster.Initialize(Settings);
            Monitoring.Initialize(Settings);
            FileSystem.Initialize(Settings);
            Counter.Initialize(Settings);
            TimeSeries.Initialize(Settings);
            Expiration.Initialize(Settings);

            if (Settings["Raven/MaxServicePointIdleTime"] != null)
                ServicePointManager.MaxServicePointIdleTime = Convert.ToInt32(Settings["Raven/MaxServicePointIdleTime"]);

            if (ConcurrentMultiGetRequests == null)
                ConcurrentMultiGetRequests = new SemaphoreSlim(Server.MaxConcurrentMultiGetRequests);

            PostInit();

            return this;
        }

        /// <summary>
        /// This limits the number of concurrent multi get requests,
        /// Note that this plays with the max number of requests allowed as well as the max number
        /// of sessions
        /// </summary>
        [JsonIgnore]
        public SemaphoreSlim ConcurrentMultiGetRequests;

        private void CheckDirectoryPermissions()
        {
            var tempPath = Core.TempPath;
            var tempFileName = Guid.NewGuid().ToString("N");
            var tempFilePath = Path.Combine(tempPath, tempFileName);

            try
            {
                IOExtensions.CreateDirectoryIfNotExists(tempPath);
                File.WriteAllText(tempFilePath, string.Empty);
                File.Delete(tempFilePath);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException(string.Format("Could not access temp path '{0}'. Please check if you have sufficient privileges to access this path or change 'Raven/TempPath' value.", tempPath), e);
            }
        }

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

        internal static ComposablePartCatalog GetUnfilteredCatalogs(ICollection<ComposablePartCatalog> catalogs)
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
                return new UriBuilder(Encryption.UseSsl ? "https" : "http", (Core.HostName ?? Environment.MachineName), Core.Port, Core.VirtualDirectory).Uri.ToString();
            }
        }

        #region Index settings

        /// <summary>
        /// The indexing scheduler to use
        /// </summary>
        public IIndexingClassifier IndexingClassifier { get; set; }

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

        [JsonIgnore]
        public AggregateCatalog Catalog { get; set; }

        public bool RunInUnreliableYetFastModeThatIsNotSuitableForProduction { get; set; }
        
        private int? maxNumberOfParallelIndexTasks;

        //this is static so repeated initializations in the same process would not trigger reflection on all MEF plugins
        private readonly static AssemblyCatalog CurrentAssemblyCatalog = new AssemblyCatalog(typeof (DocumentDatabase).Assembly);

        internal bool IsTenantDatabase { get; set; }

        public bool EnableResponseLoggingForEmbeddedDatabases { get; set; }

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

        public Uri GetFullUrl(string baseUrl)
        {
            baseUrl = Uri.EscapeUriString(baseUrl);

            if (baseUrl.StartsWith("/"))
                baseUrl = baseUrl.Substring(1);

            var url = Core.VirtualDirectory.EndsWith("/") ? Core.VirtualDirectory + baseUrl : Core.VirtualDirectory + "/" + baseUrl;
            return new Uri(url, UriKind.RelativeOrAbsolute);
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

            if (string.IsNullOrEmpty(Settings[GetKey(x => x.Storage.JournalsStoragePath)]) == false)
                Settings[GetKey(x => x.Storage.JournalsStoragePath)] = Path.Combine(Settings[GetKey(x => x.Storage.JournalsStoragePath)], "Databases", tenantId);

            if (string.IsNullOrEmpty(Settings["Raven/Voron/TempPath"]) == false)
                Settings["Raven/Voron/TempPath"] = Path.Combine(Settings["Raven/Voron/TempPath"], "Databases", tenantId, "VoronTemp");
        }

        public void CustomizeValuesForFileSystemTenant(string tenantId)
        {                                             
            if (string.IsNullOrEmpty(Settings[GetKey(x => x.FileSystem.DataDirectory)]) == false)
                Settings[GetKey(x => x.FileSystem.DataDirectory)] = Path.Combine(Settings[GetKey(x => x.FileSystem.DataDirectory)], "FileSystems", tenantId);
        }

        public void CustomizeValuesForCounterStorageTenant(string tenantId)
        {
            if (string.IsNullOrEmpty(Settings[GetKey(x => x.Counter.DataDirectory)]) == false)
                Settings[GetKey(x => x.Counter.DataDirectory)] = Path.Combine(Settings[GetKey(x => x.Counter.DataDirectory)], "Counters", tenantId);
        }

        public void CustomizeValuesForTimeSeriesTenant(string tenantId)
        {
            if (string.IsNullOrEmpty(Settings[GetKey(x => x.TimeSeries.DataDirectory)]) == false)
                Settings[GetKey(x => x.TimeSeries.DataDirectory)] = Path.Combine(Settings[GetKey(x => x.TimeSeries.DataDirectory)], "TimeSeries", tenantId);
        }

        public void CopyParentSettings(InMemoryRavenConfiguration defaultConfiguration)
        {
            Core.Port = defaultConfiguration.Core.Port;
            OAuth.TokenKey = defaultConfiguration.OAuth.TokenKey;
            OAuth.TokenServer = defaultConfiguration.OAuth.TokenServer;

            FileSystem.MaximumSynchronizationInterval = defaultConfiguration.FileSystem.MaximumSynchronizationInterval;

            Encryption.UseSsl = defaultConfiguration.Encryption.UseSsl;
            Encryption.UseFips = defaultConfiguration.Encryption.UseFips;

            Core.AssembliesDirectory = defaultConfiguration.Core.AssembliesDirectory;
            Storage.AllowOn32Bits = defaultConfiguration.Storage.AllowOn32Bits;
        }

        public IEnumerable<string> GetConfigOptionsDocs()
        {
            return ConfigOptionDocs.OptionsDocs;
        }

        public static string GetKey(Expression<Func<InMemoryRavenConfiguration, object>> getKey)
        {
            var prop = getKey.ToProperty();
            return prop.GetCustomAttributes<ConfigurationEntryAttribute>().First().Key;
        }

        public abstract class ConfigurationBase
        {
            public const string DefaultValueSetInConstructor = "default-value-set-in-constructor";

            public virtual void Initialize(NameValueCollection settings)
            {
                var properties = GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance); //TODO arek

                foreach (var property in properties)
                {
                    var entries = property.GetCustomAttributes<ConfigurationEntryAttribute>().ToList();

                    if (entries.Count == 0)
                        continue;

                    TimeUnitAttribute timeUnit = null;
                    SizeUnitAttribute sizeUnit = null;

                    if (property.PropertyType == TimeSetting.TypeOf || property.PropertyType == TimeSetting.NullableTypeOf)
                    {
                        timeUnit = property.GetCustomAttribute<TimeUnitAttribute>();
                        Debug.Assert(timeUnit != null);
                    }
                    else if (property.PropertyType == Size.TypeOf || property.PropertyType == Size.NullableTypeOf)
                    {
                        sizeUnit = property.GetCustomAttribute<SizeUnitAttribute>();
                        Debug.Assert(sizeUnit != null);
                    }

                    var configuredValueSet = false;

                    foreach (var entry in entries)
                    {
                        var value = settings[entry.Key];

                        if (value == null)
                            continue;

                        try
                        {
                            if (timeUnit != null)
                            {
                                property.SetValue(this, new TimeSetting(Convert.ToInt64(value), timeUnit.Unit));
                            }
                            else if (sizeUnit != null)
                            {
                                property.SetValue(this, new Size(Convert.ToInt64(value), sizeUnit.Unit));
                            }
                            else
                            {
                                var minValue = property.GetCustomAttribute<MinValueAttribute>();

                                if (minValue == null)
                                {
                                    if (property.PropertyType.IsEnum)
                                    {
                                        property.SetValue(this, Enum.Parse(property.PropertyType, value, true));
                                    }
                                    else
                                    {
                                        property.SetValue(this, Convert.ChangeType(value, property.PropertyType));
                                    }
                                }
                                else
                                {
                                    if (property.PropertyType == typeof(int) || property.PropertyType == typeof(int?))
                                    {
                                        property.SetValue(this, Math.Max(Convert.ToInt32(value), minValue.Int32Value));
                                    }
                                    else
                                    {
                                        throw new NotSupportedException("Min value for " + property.PropertyType + " is not supported. Property name: " + property.Name);
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            throw new InvalidOperationException("Could not set configuration value given under the following setting: " + entry.Key, e);
                        }

                        configuredValueSet = true;
                        break;
                    }

                    if (configuredValueSet)
                        continue;

                    var defaultValue = property.GetCustomAttribute<DefaultValueAttribute>().Value;
                    
                    if (DefaultValueSetInConstructor.Equals(defaultValue))
                        continue;

                    if (timeUnit != null && defaultValue != null)
                    {
                        property.SetValue(this, new TimeSetting(Convert.ToInt64(defaultValue), timeUnit.Unit));
                    }
                    else if (sizeUnit != null && defaultValue != null)
                    {
                        property.SetValue(this, new Size(Convert.ToInt64(defaultValue), sizeUnit.Unit));
                    }
                    else
                    {
                        property.SetValue(this, defaultValue);
                    }
                }
            }

            protected object GetDefaultValue<T>(Expression<Func<T, object>> getValue)
            {
                var prop = getValue.ToProperty();
                var value = prop.GetCustomAttributes<DefaultValueAttribute>().First().Value;

                if (DefaultValueSetInConstructor.Equals(value))
                {
                    return prop.GetValue(this);
                }

                return value;
            }
        }

        public class CoreConfiguration : ConfigurationBase
        {
            private readonly InMemoryRavenConfiguration parent; // TODO arek - remove
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
                    parent.Settings[Constants.RunInMemory] = value.ToString(); //TODO arek - that is needed for DatabaseLandlord.CreateConfiguration - Settings = new NameValueCollection(parentConfiguration.Settings),
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
            [ConfigurationEntry("Raven/IndexStoragePath")] // TODO arek - add initialization order
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
            [DefaultValue((string) null)]
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
            // TODO arek
            //[ConfigurationEntry("Raven/InitialNumberOfItemsToProcessInSingleBatch")]
            //[ConfigurationEntry("Raven/InitialNumberOfItemsToIndexInSingleBatch")]
            public int InitialNumberOfItemsToProcessInSingleBatch { get; set; }

            /// <summary>
            /// The initial number of items to take when reducing a batch
            /// Default: 256 or 128 depending on CPU architecture
            /// </summary>
            //TODO arek
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
                    InitialNumberOfItemsToProcessInSingleBatch = MaxNumberOfItemsToProcessInSingleBatch == (int) GetDefaultValue<CoreConfiguration>(x => x.MaxNumberOfItemsToProcessInSingleBatch) ?
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
                    InitialNumberOfItemsToReduceInSingleBatch = MaxNumberOfItemsToReduceInSingleBatch == (int) GetDefaultValue<CoreConfiguration>(x => x.MaxNumberOfItemsToReduceInSingleBatch) ?
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

        public class ServerConfiguration : ConfigurationBase
        {
            [DefaultValue(512)]
            [ConfigurationEntry("Raven/Server/MaxConcurrentRequests")]
            [ConfigurationEntry("Raven/MaxConcurrentServerRequests")] // TODO arek - remove legacy keys
            public int MaxConcurrentRequests { get; set; }

            [DefaultValue(50)]
            [ConfigurationEntry("Raven/Server/MaxConcurrentRequestsForDatabaseDuringLoad")]
            [ConfigurationEntry("Raven/MaxConcurrentRequestsForDatabaseDuringLoad")]
            public int MaxConcurrentRequestsForDatabaseDuringLoad { get; set; }

            [DefaultValue(5)]
            [TimeUnit(TimeUnit.Seconds)]
            [ConfigurationEntry("Raven/Server/MaxTimeForTaskToWaitForDatabaseToLoadInSec")]
            [ConfigurationEntry("Raven/MaxSecondsForTaskToWaitForDatabaseToLoad")]
            public TimeSetting MaxTimeForTaskToWaitForDatabaseToLoad { get; set; }

            [DefaultValue(192)]
            [ConfigurationEntry("Raven/Server/MaxConcurrentMultiGetRequests")]
            [ConfigurationEntry("Raven/MaxConcurrentMultiGetRequests")]
            public int MaxConcurrentMultiGetRequests { get; set; }


            /// <summary>
            /// Determine the value of the Access-Control-Allow-Origin header sent by the server. 
            /// Indicates the URL of a site trusted to make cross-domain requests to this server.
            /// Allowed values: null (don't send the header), *, http://example.org (space separated if multiple sites)
            /// </summary>
            [DefaultValue((string)null)]
            [ConfigurationEntry("Raven/Server/AccessControlAllowOrigin")]
            [ConfigurationEntry("Raven/AccessControlAllowOrigin")]
            public string AccessControlAllowOriginStringValue { get; set; }

            public HashSet<string> AccessControlAllowOrigin { get; set; }

            /// <summary>
            /// Determine the value of the Access-Control-Max-Age header sent by the server.
            /// Indicates how long (seconds) the browser should cache the Access Control settings.
            /// Ignored if AccessControlAllowOrigin is not specified.
            /// Default: 1728000 (20 days)
            /// </summary>
            [DefaultValue("1728000" /* 20 days */)]
            [ConfigurationEntry("Raven/Server/AccessControlMaxAge")]
            [ConfigurationEntry("Raven/AccessControlMaxAge")]
            public string AccessControlMaxAge { get; set; }

            /// <summary>
            /// Determine the value of the Access-Control-Allow-Methods header sent by the server.
            /// Indicates which HTTP methods (verbs) are permitted for requests from allowed cross-domain origins.
            /// Ignored if AccessControlAllowOrigin is not specified.
            /// Default: PUT,PATCH,GET,DELETE,POST
            /// </summary>
            [DefaultValue("PUT,PATCH,GET,DELETE,POST")]
            [ConfigurationEntry("Raven/Server/AccessControlAllowMethods")]
            [ConfigurationEntry("Raven/AccessControlAllowMethods")]
            public string AccessControlAllowMethods { get; set; }

            /// <summary>
            /// Determine the value of the Access-Control-Request-Headers header sent by the server.
            /// Indicates which HTTP headers are permitted for requests from allowed cross-domain origins.
            /// Ignored if AccessControlAllowOrigin is not specified.
            /// Allowed values: null (allow whatever headers are being requested), HTTP header field name
            /// </summary>
            [DefaultValue((string)null)]
            [ConfigurationEntry("Raven/Server/AccessControlRequestHeaders")]
            [ConfigurationEntry("Raven/AccessControlRequestHeaders")]
            public string AccessControlRequestHeaders { get; set; }

            [DefaultValue((string)null)]
            [ConfigurationEntry("Raven/Server/RedirectStudioUrl")]
            [ConfigurationEntry("Raven/RedirectStudioUrl")]
            public string RedirectStudioUrl { get; set; }

            /// <summary>
            /// The server name
            /// </summary>
            [DefaultValue((string)null)]
            [ConfigurationEntry("Raven/Server/Name")]
            [ConfigurationEntry("Raven/ServerName")]
            public string Name { get; set; }

            public override void Initialize(NameValueCollection settings)
            {
                base.Initialize(settings);

                AccessControlAllowOrigin = string.IsNullOrEmpty(AccessControlAllowOriginStringValue) ? new HashSet<string>() : new HashSet<string>(AccessControlAllowOriginStringValue.Split());
            }
        }

        public class MemoryConfiguration : ConfigurationBase
        {
            public MemoryConfiguration()
            {
                // we allow 1 GB by default, or up to 75% of available memory on startup, if less than that is available
                LimitForProcessing = Size.Min(new Size(1024, SizeUnit.Megabytes), MemoryStatistics.AvailableMemory * 0.75);

                LowMemoryForLinuxDetection = Size.Min(new Size(16, SizeUnit.Megabytes), MemoryStatistics.AvailableMemory * 0.10);

                MemoryCacheLimit = new Size(GetDefaultMemoryCacheLimitMegabytes(), SizeUnit.Megabytes);

                MemoryCacheLimitCheckInterval = new TimeSetting((long) MemoryCache.Default.PollingInterval.TotalSeconds, TimeUnit.Seconds);

                AvailableMemoryForRaisingBatchSizeLimit = Size.Min(new Size(768, SizeUnit.Megabytes), MemoryStatistics.TotalPhysicalMemory / 2);
            }

            /// <summary>
            /// Limit of how much memory a batch processing can take (in MBytes)
            /// </summary>
            [DefaultValue(DefaultValueSetInConstructor)]
            [SizeUnit(SizeUnit.Megabytes)]
            [ConfigurationEntry("Raven/Memory/LimitForProcessingInMB")]
            [ConfigurationEntry("Raven/MemoryLimitForProcessing")]
            [ConfigurationEntry("Raven/MemoryLimitForIndexing")]
            public Size LimitForProcessing { get; set; }

            public Size DynamicLimitForProcessing
            {
                get
                {
                    var availableMemory = MemoryStatistics.AvailableMemory;
                    var minFreeMemory = LimitForProcessing * 2L;
                    // we have more memory than the twice the limit, we can use the default limit
                    if (availableMemory > minFreeMemory)
                        return LimitForProcessing;

                    // we don't have enough room to play with, if two databases will request the max memory limit
                    // at the same time, we'll start paging because we'll run out of free memory. 
                    // Because of that, we'll dynamically adjust the amount
                    // of memory available for processing based on the amount of memory we actually have available,
                    // assuming that we have multiple concurrent users of memory at the same time.
                    // we limit that at 16 MB, if we have less memory than that, we can't really do much anyway
                    return Size.Min(availableMemory / 4, new Size(16, SizeUnit.Megabytes));
                }
            }

            // <summary>
            /// Limit for low mem detection in linux
            /// </summary>
            [DefaultValue(DefaultValueSetInConstructor)]
            [SizeUnit(SizeUnit.Megabytes)]
            [ConfigurationEntry("Raven/Memory/LowMemoryLimitForLinuxDetectionInMB")]
            [ConfigurationEntry("Raven/LowMemoryLimitForLinuxDetectionInMB")]
            public Size LowMemoryForLinuxDetection { get; set; }

            /// <summary>
            /// An integer value that specifies the maximum allowable size, in megabytes, that caching 
            /// document instances will use
            /// </summary>
            [DefaultValue(DefaultValueSetInConstructor)]
            [SizeUnit(SizeUnit.Megabytes)]
            [ConfigurationEntry("Raven/Memory/MemoryCacheLimitInMB")]
            [ConfigurationEntry("Raven/MemoryCacheLimitMegabytes")]
            public Size MemoryCacheLimit { get; set; }

            /// <summary>
            /// The expiration value for documents in the internal managed cache
            /// </summary>
            [DefaultValue(360)]
            [TimeUnit(TimeUnit.Seconds)]
            [ConfigurationEntry("Raven/Memory/MemoryCacheExpirationInSec")]
            [ConfigurationEntry("Raven/MemoryCacheExpiration")]
            public TimeSetting MemoryCacheExpiration { get; set; }

            /// <summary>
            /// Percentage of physical memory used for caching
            /// Allowed values: 0-99 (0 = autosize)
            /// </summary>
            [DefaultValue(0 /* auto size */)]
            [ConfigurationEntry("Raven/Memory/MemoryCacheLimitPercentage")]
            [ConfigurationEntry("Raven/MemoryCacheLimitPercentage")]
            public int MemoryCacheLimitPercentage { get; set; }

            /// <summary>
            /// Interval for checking the memory cache limits
            /// Allowed values: max precision is 1 second
            /// Default: 00:02:00 (or value provided by system.runtime.caching app config)
            /// </summary>
            [DefaultValue(DefaultValueSetInConstructor)]
            [TimeUnit(TimeUnit.Seconds)]
            [ConfigurationEntry("Raven/Memory/MemoryCacheLimitCheckIntervalInSec")]
            [ConfigurationEntry("Raven/MemoryCacheLimitCheckInterval")]
            public TimeSetting MemoryCacheLimitCheckInterval { get; set; }

            [DefaultValue(DefaultValueSetInConstructor)]
            [SizeUnit(SizeUnit.Megabytes)]
            [ConfigurationEntry("Raven/Memory/AvailableMemoryForRaisingBatchSizeLimitInMB")]
            [ConfigurationEntry("Raven/AvailableMemoryForRaisingBatchSizeLimit")]
            [ConfigurationEntry("Raven/AvailableMemoryForRaisingIndexBatchSizeLimit")]
            public Size AvailableMemoryForRaisingBatchSizeLimit { get; set; }

            private int GetDefaultMemoryCacheLimitMegabytes()
            {
                // TODO: This used to use an esent key. Ensure that this is not needed anymore and kill this method. 
                var cacheSizeMaxSetting = 1024;

                // we need to leave ( a lot ) of room for other things as well, so we min the cache size
                int val = (int) ((MemoryStatistics.TotalPhysicalMemory.ValueInBytes / 2) -
                                 // reduce the unmanaged cache size from the default min
                                 cacheSizeMaxSetting);

                if (val < 0)
                    return 128; // if machine has less than 1024 MB, then only use 128 MB 

                return val;
            }
        }

        public class StorageConfiguration : ConfigurationBase
        {
            [DefaultValue(false)]
            [ConfigurationEntry("Raven/Storage/PreventSchemaUpdate")]
            [ConfigurationEntry("Raven/PreventSchemaUpdate")]
            public bool PreventSchemaUpdate { get; set; }

            /// <summary>
            /// You can use this setting to specify a maximum buffer pool size that can be used for transactional storage (in gigabytes). 
            /// By default it is 4.
            /// Minimum value is 2.
            /// </summary>
            [DefaultValue(4)]
            [MinValue(2)]
            [SizeUnit(SizeUnit.Gigabytes)]
            [ConfigurationEntry("Raven/Storage/MaxBufferPoolSizeInGB")]
            [ConfigurationEntry("Raven/Voron/MaxBufferPoolSize")]
            public Size MaxBufferPoolSize { get; set; }

            /// <summary>
            /// You can use this setting to specify an initial file size for data file (in bytes).
            /// </summary>
            [DefaultValue(null)]
            [SizeUnit(SizeUnit.Bytes)]
            [ConfigurationEntry("Raven/Storage/InitialFileSize")]
            [ConfigurationEntry("Raven/Voron/InitialFileSize")]
            public Size? InitialFileSize { get; set; }

            /// <summary>
            /// The maximum scratch buffer size that can be used by Voron. The value is in megabytes. 
            /// Default: 6144.
            /// </summary>
            [DefaultValue(6144)]
            [SizeUnit(SizeUnit.Megabytes)]
            [ConfigurationEntry("Raven/Storage/MaxScratchBufferSizeInMB")]
            [ConfigurationEntry("Raven/Voron/MaxScratchBufferSize")]
            public Size MaxScratchBufferSize { get; set; }

            /// <summary>
            /// The minimum number of megabytes after which each scratch buffer size increase will create a notification. Used for indexing batch size tuning.
            /// Default: 
            /// 1024 when MaxScratchBufferSize > 1024, 
            /// 512 when MaxScratchBufferSize > 512
            /// null otherwise (disabled) 
            /// </summary>
            [DefaultValue(null)]
            [SizeUnit(SizeUnit.Megabytes)]
            [ConfigurationEntry("Raven/Storage/ScratchBufferSizeNotificationThresholdInMB")]
            [ConfigurationEntry("Raven/Voron/ScratchBufferSizeNotificationThreshold")]
            public Size? ScratchBufferSizeNotificationThreshold { get; set; }

            /// <summary>
            /// If you want to use incremental backups, you need to turn this to true, but then journal files will not be deleted after applying them to the data file. They will be deleted only after a successful backup. 
            /// Default: false.
            /// </summary>
            [DefaultValue(false)]
            [ConfigurationEntry("Raven/Storage/AllowIncrementalBackups")]
            [ConfigurationEntry("Raven/Voron/AllowIncrementalBackups")]
            public bool AllowIncrementalBackups { get; set; }

            /// <summary>
            /// You can use this setting to specify a different path to temporary files. By default it is empty, which means that temporary files will be created at same location as data file.
            /// </summary>
            [DefaultValue(null)]
            [ConfigurationEntry("Raven/Storage/TempPath")]
            [ConfigurationEntry("Raven/Voron/TempPath")]
            public string TempPath { get; set; }

            [DefaultValue(null)]
            [ConfigurationEntry("Raven/Storage/TransactionJournalsPath")]
            [ConfigurationEntry("Raven/TransactionJournalsPath")]
            public string JournalsStoragePath { get; set; }

            /// <summary>
            /// Whether to allow Voron to run in 32 bits process.
            /// </summary>
            [DefaultValue(false)]
            [ConfigurationEntry("Raven/Storage/AllowOn32Bits")]
            [ConfigurationEntry("Raven/Voron/AllowOn32Bits")]
            public bool AllowOn32Bits { get; set; }

            public override void Initialize(NameValueCollection settings)
            {
                base.Initialize(settings);

                if (ScratchBufferSizeNotificationThreshold == null)
                {
                    var _1024MB = new Size(1024, SizeUnit.Megabytes);
                    var _512MB = new Size(512, SizeUnit.Megabytes);

                    if (MaxScratchBufferSize > _1024MB)
                        ScratchBufferSizeNotificationThreshold = _1024MB;
                    else if (MaxScratchBufferSize > _512MB)
                        ScratchBufferSizeNotificationThreshold = _512MB;
                }
            }
        }

        public class PrefetcherConfiguration : ConfigurationBase
        {
            public PrefetcherConfiguration()
            {
                MaxNumberOfItemsToPreFetch = CoreConfiguration.DefaultMaxNumberOfItemsToProcessInSingleBatch;
            }

            [DefaultValue(5000)]
            [TimeUnit(TimeUnit.Milliseconds)]
            [ConfigurationEntry("Raven/Prefetching/DurationLimitInMs")]
            [ConfigurationEntry("Raven/Prefetching/DurationLimit")]
            public TimeSetting DurationLimit { get; set; }

            [DefaultValue(false)]
            [ConfigurationEntry("Raven/Prefetching/Disable")]
            [ConfigurationEntry("Raven/DisableDocumentPreFetching")]
            [ConfigurationEntry("Raven/DisableDocumentPreFetchingForIndexing")]
            public bool Disabled { get; set; }

            [DefaultValue(DefaultValueSetInConstructor)]
            [MinValue(128)]
            [ConfigurationEntry("Raven/Prefetching/MaxNumberOfItemsToPreFetch")]
            [ConfigurationEntry("Raven/MaxNumberOfItemsToPreFetch")]
            [ConfigurationEntry("Raven/MaxNumberOfItemsToPreFetchForIndexing")]
            public int MaxNumberOfItemsToPreFetch { get; set; }

            /// <summary>
            /// Number of seconds after which prefetcher will stop reading documents from disk. Default: 5.
            /// </summary>
            [DefaultValue(5)]
            [TimeUnit(TimeUnit.Seconds)]
            [ConfigurationEntry("Raven/Prefetching/FetchingDocumentsFromDiskTimeoutInSec")]
            [ConfigurationEntry("Raven/Prefetcher/FetchingDocumentsFromDiskTimeout")]
            public TimeSetting FetchingDocumentsFromDiskTimeout { get; set; }

            /// <summary>
            /// Maximum number of megabytes after which prefetcher will stop reading documents from disk. Default: 256.
            /// </summary>
            [DefaultValue(256)]
            [SizeUnit(SizeUnit.Megabytes)]
            [ConfigurationEntry("Raven/Prefetching/MaximumSizeAllowedToFetchFromStorageInMB")]
            [ConfigurationEntry("Raven/Prefetcher/MaximumSizeAllowedToFetchFromStorage")]
            public Size MaximumSizeAllowedToFetchFromStorageInMb { get; set; }
        }

        public class ReplicationConfiguration : ConfigurationBase
        {
            [DefaultValue(600)]
            [TimeUnit(TimeUnit.Seconds)]
            [ConfigurationEntry("Raven/Replication/IndexAndTransformerReplicationLatencyInSec")]
            [ConfigurationEntry("Raven/Replication/IndexAndTransformerReplicationLatency")]
            public TimeSetting IndexAndTransformerReplicationLatency { get; set; }

            /// <summary>
            /// Number of seconds after which replication will stop reading documents from disk. Default: 30.
            /// </summary>
            [DefaultValue(30)]
            [TimeUnit(TimeUnit.Seconds)]
            [ConfigurationEntry("Raven/Replication/FetchingFromDiskTimeoutInSec")]
            [ConfigurationEntry("Raven/Replication/FetchingFromDiskTimeout")]
            public TimeSetting FetchingFromDiskTimeoutInSeconds { get; set; }

            /// <summary>
            /// Number of milliseconds before replication requests will timeout. Default: 60 * 1000.
            /// </summary>
            [DefaultValue(60 * 1000)]
            [TimeUnit(TimeUnit.Milliseconds)]
            [ConfigurationEntry("Raven/Replication/ReplicationRequestTimeoutInMs")]
            [ConfigurationEntry("Raven/Replication/ReplicationRequestTimeout")]
            public TimeSetting ReplicationRequestTimeout { get; set; }

            /// <summary>
            /// Force us to buffer replication requests (useful if using windows auth under certain scenarios).
            /// </summary>
            [DefaultValue(false)]
            [ConfigurationEntry("Raven/Replication/ForceReplicationRequestBuffering")]
            public bool ForceReplicationRequestBuffering { get; set; }

            /// <summary>
            /// Maximum number of items replication will receive in single batch. Min: 512. Default: null (let source server decide).
            /// </summary>
            [DefaultValue(null)]
            [MinValue(512)]
            [ConfigurationEntry("Raven/Replication/MaxNumberOfItemsToReceiveInSingleBatch")]
            public int? MaxNumberOfItemsToReceiveInSingleBatch { get; set; }
        }

        public class FileSystemConfiguration : ConfigurationBase
        {
            private CoreConfiguration core;
            public FileSystemConfiguration(CoreConfiguration coreConfiguration)
            {
                core = coreConfiguration;
            }

            private string fileSystemDataDirectory;

            private string fileSystemIndexStoragePath;

            [DefaultValue(60)]
            [TimeUnit(TimeUnit.Seconds)]
            [ConfigurationEntry("Raven/FileSystem/MaximumSynchronizationIntervalInSec")]
            [ConfigurationEntry("Raven/FileSystem/MaximumSynchronizationInterval")]
            public TimeSetting MaximumSynchronizationInterval { get; set; }

            /// <summary>
            /// The directory for the RavenDB file system. 
            /// You can use the ~\ prefix to refer to RavenDB's base directory. 
            /// </summary>
            [DefaultValue(@"~\FileSystems")]
            [ConfigurationEntry("Raven/FileSystem/DataDir")]
            public string DataDirectory
            {
                get { return fileSystemDataDirectory; }
                set { fileSystemDataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(core.WorkingDirectory, value); }
            }

            [DefaultValue("")]
            [ConfigurationEntry("Raven/FileSystem/IndexStoragePath")]
            public string IndexStoragePath
            {
                get
                {
                    if (string.IsNullOrEmpty(fileSystemIndexStoragePath))
                        fileSystemIndexStoragePath = Path.Combine(DataDirectory, "Indexes");
                    return fileSystemIndexStoragePath;
                }
                set
                {
                    fileSystemIndexStoragePath = value.ToFullPath();
                }
            }
        }

        public class QueryConfiguration : ConfigurationBase
        {
            [DefaultValue(1024)] //1024 is Lucene.net default - so if the setting is not set it will be the same as not touching Lucene's settings at all
            [ConfigurationEntry("Raven/Query/MaxClauseCount")]
            [ConfigurationEntry("Raven/MaxClauseCount")]
            public int MaxClauseCount { get; set; }
        }

        public class PatchingConfiguration : ConfigurationBase
        {
            [DefaultValue(false)]
            [ConfigurationEntry("Raven/Patching/AllowScriptsToAdjustNumberOfSteps")]
            [ConfigurationEntry("Raven/AllowScriptsToAdjustNumberOfSteps")]
            public bool AllowScriptsToAdjustNumberOfSteps { get; set; }

            /// <summary>
            /// The maximum number of steps (instructions) to give a script before timing out.
            /// Default: 10,000
            /// </summary>
            [DefaultValue(10 * 1000)]
            [ConfigurationEntry("Raven/Patching/MaxStepsForScript")]
            [ConfigurationEntry("Raven/MaxStepsForScript")]
            public int MaxStepsForScript { get; set; }

            /// <summary>
            /// The number of additional steps to add to a given script based on the processed document's quota.
            /// Set to 0 to give use a fixed size quota. This value is multiplied with the doucment size.
            /// Default: 5
            /// </summary>
            [DefaultValue(5)]
            [ConfigurationEntry("Raven/AdditionalStepsForScriptBasedOnDocumentSize")]
            public int AdditionalStepsForScriptBasedOnDocumentSize { get; set; }
        }

        public class BulkInsertConfiguration : ConfigurationBase
        {
            [DefaultValue(60000)]
            [TimeUnit(TimeUnit.Milliseconds)]
            [ConfigurationEntry("Raven/BulkImport/BatchTimeoutInMs")]
            [ConfigurationEntry("Raven/BulkImport/BatchTimeout")]
            public TimeSetting ImportBatchTimeout { get; set; }
        }

        public class CounterConfiguration : ConfigurationBase
        {
            private readonly CoreConfiguration coreConfiguration;

            public CounterConfiguration(CoreConfiguration coreConfiguration)
            {
                this.coreConfiguration = coreConfiguration;
            }


            private string countersDataDirectory;

            /// <summary>
            /// The directory for the RavenDB counters. 
            /// You can use the ~\ prefix to refer to RavenDB's base directory. 
            /// </summary>
            [DefaultValue(@"~\Counters")]
            [ConfigurationEntry("Raven/Counter/DataDir")]
            [ConfigurationEntry("Raven/Counters/DataDir")]
            public string DataDirectory
            {
                get { return countersDataDirectory; }
                set { countersDataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(coreConfiguration.WorkingDirectory, value); }
            }

            /// <summary>
            /// Determines how long tombstones will be kept by a counter storage. After the specified time they will be automatically
            /// Purged on next counter storage startup. Default: 14 days.
            /// </summary>
            [DefaultValue(14)]
            [TimeUnit(TimeUnit.Days)]
            [ConfigurationEntry("Raven/Counter/TombstoneRetentionTimeInDays")]
            [ConfigurationEntry("Raven/Counter/TombstoneRetentionTime")]
            public TimeSetting TombstoneRetentionTime { get; set; }

            [DefaultValue(1000)]
            [ConfigurationEntry("Raven/Counter/DeletedTombstonesInBatch")]
            public int DeletedTombstonesInBatch { get; set; }

            [DefaultValue(30 * 1000)]
            [TimeUnit(TimeUnit.Milliseconds)]
            [ConfigurationEntry("Raven/Counter/ReplicationLatency")]
            public TimeSetting ReplicationLatency { get; set; }
        }

        public class TimeSeriesConfiguration : ConfigurationBase
        {
            private readonly CoreConfiguration coreConfiguration;

            public TimeSeriesConfiguration(CoreConfiguration coreConfiguration)
            {
                this.coreConfiguration = coreConfiguration;
            }

            private string timeSeriesDataDirectory;

            /// <summary>
            /// The directory for the RavenDB time series. 
            /// You can use the ~\ prefix to refer to RavenDB's base directory. 
            /// </summary>
            [DefaultValue(@"~\TimeSeries")]
            [ConfigurationEntry("Raven/TimeSeries/DataDir")]
            public string DataDirectory
            {
                get { return timeSeriesDataDirectory; }
                set { timeSeriesDataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(coreConfiguration.WorkingDirectory, value); }
            }

            /// <summary>
            /// Determines how long tombstones will be kept by a time series. After the specified time they will be automatically
            /// Purged on next time series startup. Default: 14 days.
            /// </summary>
            [DefaultValue(14)]
            [TimeUnit(TimeUnit.Days)]
            [ConfigurationEntry("Raven/TimeSeries/TombstoneRetentionTimeInDays")]
            [ConfigurationEntry("Raven/TimeSeries/TombstoneRetentionTime")]
            public TimeSetting TombstoneRetentionTime { get; set; }

            [DefaultValue(1000)]
            [ConfigurationEntry("Raven/TimeSeries/DeletedTombstonesInBatch")]
            public int DeletedTombstonesInBatch { get; set; }

            [DefaultValue(30 * 1000)]
            [TimeUnit(TimeUnit.Milliseconds)]
            [ConfigurationEntry("Raven/TimeSeries/ReplicationLatency")]
            public TimeSetting ReplicationLatency { get; set; }
        }

        public class EncryptionConfiguration : ConfigurationBase
        {
            /// <summary>
            /// Whatever we should use FIPS compliant encryption algorithms
            /// </summary>
            [DefaultValue(false)]
            [ConfigurationEntry("Raven/Encryption/FIPS")]
            public bool UseFips { get; set; }

            [DefaultValue(128)]
            [ConfigurationEntry("Raven/Encryption/KeyBitsPreference")]
            public int EncryptionKeyBitsPreference { get; set; }

            /// <summary>
            /// Whatever we should use SSL for this connection
            /// </summary>
            [DefaultValue(false)]
            [ConfigurationEntry("Raven/UseSsl")]
            public bool UseSsl { get; set; }

            public override void Initialize(NameValueCollection settings)
            {
                base.Initialize(settings);
          
                //TODO arek - verify that
                //if (string.IsNullOrEmpty(DatabaseName)) // we only use this for root database
                //{
                //    Encryption.UseSsl = ravenSettings.Encryption.UseSsl.Value;
                //    Encryption.UseFips = ravenSettings.Encryption.UseFips.Value;
                //}
            }
        }

        public class IndexingConfiguration : ConfigurationBase
        {
            private bool useLuceneASTParser = true;

            [DefaultValue(256 * 1024)]
            [ConfigurationEntry("Raven/Indexing/MaxWritesBeforeRecreate")]
            [ConfigurationEntry("Raven/MaxIndexWritesBeforeRecreate")]
            public int MaxWritesBeforeRecreate { get; set; }

            /// <summary>
            /// Limits the number of map outputs that a simple index is allowed to create for a one source document. If a map operation applied to the one document
            /// produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and
            /// the appropriate error message will be added to the indexing errors.
            /// Default value: 15. In order to disable this check set value to -1.
            /// </summary>
            [DefaultValue(15)]
            [ConfigurationEntry("Raven/Indexing/MaxSimpleIndexOutputsPerDocument")]
            [ConfigurationEntry("Raven/MaxSimpleIndexOutputsPerDocument")]
            public int MaxSimpleIndexOutputsPerDocument { get; set; }

            /// <summary>
            /// Limits the number of map outputs that a map-reduce index is allowed to create for a one source document. If a map operation applied to the one document
            /// produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and
            /// the appropriate error message will be added to the indexing errors.
            /// Default value: 50. In order to disable this check set value to -1.
            /// </summary>
            [DefaultValue(50)]
            [ConfigurationEntry("Raven/Indexing/MaxMapReduceIndexOutputsPerDocument")]
            [ConfigurationEntry("Raven/MaxMapReduceIndexOutputsPerDocument")]
            public int MaxMapReduceIndexOutputsPerDocument { get; set; }

            /// <summary>
            /// How long can we keep the new index in memory before we have to flush it
            /// </summary>
            [DefaultValue(15)]
            [TimeUnit(TimeUnit.Minutes)]
            [ConfigurationEntry("Raven/Indexing/NewIndexInMemoryMaxTimeInMin")]
            [ConfigurationEntry("Raven/NewIndexInMemoryMaxTime")]
            public TimeSetting NewIndexInMemoryMaxTime { get; set; }

            /// <summary>
            /// New indexes are kept in memory until they reach this integer value in bytes or until they're non-stale
            /// Default: 64 MB
            /// Minimum: 1 MB
            /// </summary>
            [DefaultValue(64)]
            [MinValue(1)]
            [SizeUnit(SizeUnit.Megabytes)]
            [ConfigurationEntry("Raven/Indexing/NewIndexInMemoryMaxInMB")]
            [ConfigurationEntry("Raven/NewIndexInMemoryMaxMB")]
            public Size NewIndexInMemoryMaxSize { get; set; }

            /// <summary>
            /// Controls whatever RavenDB will create temporary indexes 
            /// for queries that cannot be directed to standard indexes
            /// </summary>
            [DefaultValue(true)]
            [ConfigurationEntry("Raven/Indexing/CreateAutoIndexesForAdHocQueriesIfNeeded")]
            [ConfigurationEntry("Raven/CreateAutoIndexesForAdHocQueriesIfNeeded")]
            public bool CreateAutoIndexesForAdHocQueriesIfNeeded { get; set; }

            /// <summary>
            /// When the database is shut down rudely, determine whatever to reset the index or to check it.
            /// Checking the index may take some time on large databases
            /// </summary>
            [DefaultValue(false)]
            [ConfigurationEntry("Raven/Indexing/ResetIndexOnUncleanShutdown")]
            [ConfigurationEntry("Raven/ResetIndexOnUncleanShutdown")]
            public bool ResetIndexOnUncleanShutdown { get; set; }

            /// <summary>
            /// Prevent index from being kept in memory. Default: false
            /// </summary>
            [DefaultValue(false)]
            [ConfigurationEntry("Raven/Indexing/DisableInMemory")]
            [ConfigurationEntry("Raven/DisableInMemoryIndexing")]
            public bool DisableInMemoryIndexing { get; set; }

            /// <summary>
            /// Maximum time interval for storing commit points for map indexes when new items were added.
            /// The commit points are used to restore index if unclean shutdown was detected.
            /// Default: 00:05:00 
            /// </summary>
            [DefaultValue(5)]
            [TimeUnit(TimeUnit.Minutes)]
            [ConfigurationEntry("Raven/Indexing/MaxIndexCommitPointStoreIntervalInMin")]
            [ConfigurationEntry("Raven/MaxIndexCommitPointStoreTimeInterval")]
            public TimeSetting MaxIndexCommitPointStoreInterval { get; set; }

            /// <summary>
            /// Maximum number of kept commit points to restore map index after unclean shutdown
            /// Default: 5
            /// </summary>
            [DefaultValue(5)]
            [ConfigurationEntry("Raven/Indexing/MaxNumberOfStoredCommitPoints")]
            [ConfigurationEntry("Raven/MaxNumberOfStoredCommitPoints")]
            public int MaxNumberOfStoredCommitPoints { get; set; }

            /// <summary>
            /// Minimum interval between between successive indexing that will allow to store a  commit point
            /// Default: 00:01:00
            /// </summary>
            [DefaultValue(1)]
            [TimeUnit(TimeUnit.Minutes)]
            [ConfigurationEntry("Raven/Indexing/MinIndexingIntervalToStoreCommitPointInMin")]
            [ConfigurationEntry("Raven/MinIndexingTimeIntervalToStoreCommitPoint")]
            public TimeSetting MinIndexingIntervalToStoreCommitPoint { get; set; }

            [DefaultValue(10)]
            [TimeUnit(TimeUnit.Minutes)]
            [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeRunningIdleIndexesInMin")]
            [ConfigurationEntry("Raven/TimeToWaitBeforeRunningIdleIndexes")]
            public TimeSetting TimeToWaitBeforeRunningIdleIndexes { get; internal set; }

            [DefaultValue(60)]
            [TimeUnit(TimeUnit.Minutes)]
            [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeMarkingAutoIndexAsIdleInMin")]
            [ConfigurationEntry("Raven/TimeToWaitBeforeMarkingAutoIndexAsIdle")]
            public TimeSetting TimeToWaitBeforeMarkingAutoIndexAsIdle { get; set; }

            [DefaultValue(72)]
            [TimeUnit(TimeUnit.Hours)]
            [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeMarkingIdleIndexAsAbandonedInHrs")]
            [ConfigurationEntry("Raven/TimeToWaitBeforeMarkingIdleIndexAsAbandoned")]
            public TimeSetting TimeToWaitBeforeMarkingIdleIndexAsAbandoned { get; set; }

            [DefaultValue(3)]
            [TimeUnit(TimeUnit.Hours)]
            [ConfigurationEntry("Raven/Indexing/TimeToWaitBeforeRunningAbandonedIndexesInHrs")]
            [ConfigurationEntry("Raven/TimeToWaitBeforeRunningAbandonedIndexes")]
            public TimeSetting TimeToWaitBeforeRunningAbandonedIndexes { get; set; }

            [DefaultValue(512)]
            [ConfigurationEntry("Raven/Indexing/MaxNumberOfItemsToProcessInTestIndexes")]
            public int MaxNumberOfItemsToProcessInTestIndexes { get; set; }

            [DefaultValue(2048)]
            [ConfigurationEntry("Raven/Indexing/DisableIndexingFreeSpaceThreshold")]
            public int DisableIndexingFreeSpaceThreshold { get; set; }

            [DefaultValue(false)]
            [ConfigurationEntry("Raven/Indexing/DisableMapReduceInMemoryTracking")]
            public bool DisableMapReduceInMemoryTracking { get; set; }

            [DefaultValue(512)]
            [ConfigurationEntry("Raven/Indexing/MaxNumberOfStoredIndexingBatchInfoElements")]
            public int MaxNumberOfStoredIndexingBatchInfoElements { get; set; }

            [DefaultValue(true)]
            [ConfigurationEntry("Raven/Indexing/UseLuceneASTParser")]
            public bool UseLuceneASTParser
            {
                get { return useLuceneASTParser; }
                set
                {
                    if (value == useLuceneASTParser)
                        return;
                    QueryBuilder.UseLuceneASTParser = useLuceneASTParser = value;
                }
            }

            /// <summary>
            /// Indexes are flushed to a disk only if their in-memory size exceed the specified value. Default: 5MB
            /// </summary>
            [DefaultValue(5)]
            [SizeUnit(SizeUnit.Megabytes)]
            [ConfigurationEntry("Raven/Indexing/FlushIndexToDiskSizeInMB")]
            public Size FlushIndexToDiskSize { get; set; }
        }

        public class ClusterConfiguration : ConfigurationBase
        {
            public ClusterConfiguration()
            {
                MaxStepDownDrainTime = new TimeSetting((long) RaftEngineOptions.DefaultMaxStepDownDrainTime.TotalSeconds, TimeUnit.Seconds);
            }

            [DefaultValue(RaftEngineOptions.DefaultElectionTimeout * 5)] // 6000ms
            [TimeUnit(TimeUnit.Milliseconds)]
            [ConfigurationEntry("Raven/Cluster/ElectionTimeoutInMs")]
            [ConfigurationEntry("Raven/Cluster/ElectionTimeout")]
            public TimeSetting ElectionTimeout { get; set; }

            [DefaultValue(RaftEngineOptions.DefaultHeartbeatTimeout * 5)] // 1500ms
            [TimeUnit(TimeUnit.Milliseconds)]
            [ConfigurationEntry("Raven/Cluster/HeartbeatTimeoutInMs")]
            [ConfigurationEntry("Raven/Cluster/HeartbeatTimeout")]
            public TimeSetting HeartbeatTimeout { get; set; }

            [DefaultValue(RaftEngineOptions.DefaultMaxLogLengthBeforeCompaction)]
            [ConfigurationEntry("Raven/Cluster/MaxLogLengthBeforeCompaction")]
            public int MaxLogLengthBeforeCompaction { get; set; }

            [DefaultValue(DefaultValueSetInConstructor)]
            [TimeUnit(TimeUnit.Seconds)]
            [ConfigurationEntry("Raven/Cluster/MaxStepDownDrainTime")]
            public TimeSetting MaxStepDownDrainTime { get; set; }

            [DefaultValue(RaftEngineOptions.DefaultMaxEntiresPerRequest)]
            [ConfigurationEntry("Raven/Cluster/MaxEntriesPerRequest")]
            public int MaxEntriesPerRequest { get; set; }
        }

        public class MonitoringConfiguration : ConfigurationBase
        {
            public MonitoringConfiguration()
            {
                Snmp = new SnmpConfiguration();
            }

            public SnmpConfiguration Snmp { get; private set; }

            public override void Initialize(NameValueCollection settings)
            {
                Snmp.Initialize(settings);
            }

            public class SnmpConfiguration : ConfigurationBase
            {
                [DefaultValue(false)]
                [ConfigurationEntry("Raven/Monitoring/Snmp/Enabled")]
                public bool Enabled { get; set; }

                [DefaultValue(161)]
                [ConfigurationEntry("Raven/Monitoring/Snmp/Port")]
                public int Port { get; set; }

                [DefaultValue("ravendb")]
                [ConfigurationEntry("Raven/Monitoring/Snmp/Community")]
                public string Community { get; set; }
            }
        }

        public class WebSocketsConfiguration
        {
            [DefaultValue(128 * 1024)]
            [SizeUnit(SizeUnit.Bytes)]
            [ConfigurationEntry("Raven/WebSockets/InitialBufferPoolSize")]
            public Size InitialBufferPoolSize { get; set; }
        }

        public class OAuthConfiguration : ConfigurationBase
        {
            public OAuthConfiguration(string serverUrl)
            {
                TokenServer = serverUrl.EndsWith("/") ? serverUrl + "OAuth/API-Key" : serverUrl + "/OAuth/API-Key";
            }

            [DefaultValue(DefaultValueSetInConstructor)]
            [ConfigurationEntry("Raven/OAuthTokenServer")]
            public string TokenServer { get; set; }

            public bool UseDefaultTokenServer { get; private set; }

            /// <summary>
            /// The certificate to use when verifying access token signatures for OAuth
            /// </summary>
            public byte[] TokenKey { get; set; }

            public override void Initialize(NameValueCollection settings)
            {
                base.Initialize(settings);

                TokenKey = GetOAuthKey(settings);
                UseDefaultTokenServer = settings[GetKey(x => x.OAuth.TokenServer)] == null;
            }

            private static readonly Lazy<byte[]> DefaultOauthKey = new Lazy<byte[]>(() =>
            {
                using (var rsa = Encryptor.Current.CreateAsymmetrical())
                {
                    return rsa.ExportCspBlob(true);
                }
            });

            private byte[] GetOAuthKey(NameValueCollection settings)
            {
                var key = settings["Raven/OAuthTokenCertificate"];
                if (string.IsNullOrEmpty(key) == false)
                {
                    return Convert.FromBase64String(key);
                }
                return DefaultOauthKey.Value; // ensure we only create this once per process
            }
        }

        public class ExpirationBundleConfiguration : ConfigurationBase
        {
            [DefaultValue(300)]
            [TimeUnit(TimeUnit.Seconds)]
            [ConfigurationEntry("Raven/Expiration/DeleteFrequencyInSec")]
            [ConfigurationEntry("Raven/Expiration/DeleteFrequencySeconds")]
            public TimeSetting DeleteFrequency { get; set; }
        }
    }
}
