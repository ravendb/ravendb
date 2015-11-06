//-----------------------------------------------------------------------
// <copyright file="InMemoryRavenConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel.Composition.Hosting;
using System.ComponentModel.Composition.Primitives;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Plugins.Catalogs;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Extensions;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Categories;

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

        public PatchingConfiguration Patching { get; }

        public BulkInsertConfiguration BulkInsert { get; }

        public ServerConfiguration Server { get; }

        public MemoryConfiguration Memory { get; }

        public OAuthConfiguration OAuth { get; private set; }

        public ExpirationBundleConfiguration Expiration { get; }

        public VersioningBundleConfiguration Versioning { get; }

        public StudioConfiguration Studio { get; }

        public TenantConfiguration Tenants { get; }

        public LicenseConfiguration Licensing { get; }

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
            Versioning = new VersioningBundleConfiguration();
            Studio = new StudioConfiguration();
            Tenants = new TenantConfiguration();
            Licensing = new LicenseConfiguration();

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
            Versioning.Initialize(Settings);
            Studio.Initialize(Settings);
            Tenants.Initialize(Settings);
            Licensing.Initialize(Settings);

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
                if (httpRequest != null) // running in IIS, let us figure out how
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

        /// <summary>
        /// The indexing scheduler to use
        /// </summary>
        public IIndexingClassifier IndexingClassifier { get; set; }

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

        //this is static so repeated initializations in the same process would not trigger reflection on all MEF plugins
        private static readonly AssemblyCatalog CurrentAssemblyCatalog = new AssemblyCatalog(typeof(DocumentDatabase).Assembly);

        internal bool IsTenantDatabase { get; set; }

        public bool EnableResponseLoggingForEmbeddedDatabases { get; set; }

        internal void ResetContainer()
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
            if (string.IsNullOrEmpty(Settings[GetKey(x => x.Core.IndexStoragePath)]) == false)
                Settings[GetKey(x => x.Core.IndexStoragePath)] = Path.Combine(Settings[GetKey(x => x.Core.IndexStoragePath)], "Databases", tenantId);

            if (string.IsNullOrEmpty(Settings[GetKey(x => x.Storage.JournalsStoragePath)]) == false)
                Settings[GetKey(x => x.Storage.JournalsStoragePath)] = Path.Combine(Settings[GetKey(x => x.Storage.JournalsStoragePath)], "Databases", tenantId);

            if (string.IsNullOrEmpty(Settings[GetKey(x => x.Storage.TempPath)]) == false)
                Settings[GetKey(x => x.Storage.TempPath)] = Path.Combine(Settings[GetKey(x => x.Storage.TempPath)], "Databases", tenantId, "VoronTemp");
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

    }
}
