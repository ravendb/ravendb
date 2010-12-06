using System;
using System.Collections.Specialized;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Linq;
using log4net.Config;
using Raven.Database.Extensions;
using Raven.Database.Storage;
using Raven.Http;

namespace Raven.Database.Config
{
    public class InMemoryRavenConfiguration : IRaveHttpnConfiguration
    {
        private CompositionContainer container;
        private bool containerExternallySet;
        private string dataDirectory;
        private string pluginsDirectory;
        private bool runInUnreliableYetFastModeThatIsNotSuitableForProduction;

        public InMemoryRavenConfiguration()
        {
            Settings = new NameValueCollection(StringComparer.InvariantCultureIgnoreCase);


            Catalog = new AggregateCatalog(
                new AssemblyCatalog(typeof(HttpServer).Assembly),
                new AssemblyCatalog(typeof(DocumentDatabase).Assembly)
                );

            Catalog.Changed += (sender, args) => ResetContainer();
        }

        public void Initialize()
        {
            HostName = Settings["Raven/HostName"];

            var portStr = Settings["Raven/Port"];

            Port = portStr != null ? int.Parse(portStr) : 8080;

            var maxPageSizeStr = Settings["Raven/MaxPageSize"];
            var minimumQueryCount = Settings["Raven/TempIndexPromotionMinimumQueryCount"];
            var queryThreshold = Settings["Raven/TempIndexPromotionThreshold"];
            var cleanupPeriod = Settings["Raven/TempIndexCleanupPeriod"];
            var cleanupThreshold = Settings["Raven/TempIndexCleanupThreshold"];

            MaxPageSize = maxPageSizeStr != null ? int.Parse(maxPageSizeStr) : 1024;
            TempIndexPromotionMinimumQueryCount = minimumQueryCount != null ? int.Parse(minimumQueryCount) : 100;
            TempIndexPromotionThreshold = queryThreshold != null ? int.Parse(queryThreshold) : 60000; // once a minute
            TempIndexCleanupPeriod = cleanupPeriod != null ? TimeSpan.FromSeconds(int.Parse(cleanupPeriod)) : TimeSpan.FromMinutes(10);
            TempIndexCleanupThreshold = cleanupThreshold != null ? TimeSpan.FromSeconds(int.Parse(cleanupThreshold)) : TimeSpan.FromMinutes(20);

            RunInMemory = GetConfigurationValue<bool>("Raven/RunInMemory") ?? false;

            DataDirectory = Settings["Raven/DataDir"] ?? @"~\Data";

            WebDir = Settings["Raven/WebDir"] ?? GetDefaultWebDir();

            AccessControlAllowOrigin = Settings["Raven/AccessControlAllowOrigin"];

            bool httpCompressionTemp;
            if (bool.TryParse(Settings["Raven/HttpCompression"], out httpCompressionTemp) ==
                false)
                httpCompressionTemp = true;
            HttpCompression = httpCompressionTemp;

            var transactionMode = Settings["Raven/TransactionMode"];
            TransactionMode result;
            if (Enum.TryParse(transactionMode, true, out result) == false)
                result = TransactionMode.Safe;
            TransactionMode = result;

            VirtualDirectory = Settings["Raven/VirtualDirectory"] ?? "/";

            if (VirtualDirectory.EndsWith("/"))
                VirtualDirectory = VirtualDirectory.Substring(0, VirtualDirectory.Length - 1);
            if (VirtualDirectory.StartsWith("/") == false)
                VirtualDirectory = "/" + VirtualDirectory;

            PluginsDirectory = Settings["Raven/PluginsDirectory"] ?? @"~\Plugins";
            if (PluginsDirectory.StartsWith(@"~\"))
                PluginsDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, PluginsDirectory.Substring(2));

            AnonymousUserAccessMode = GetAnonymousUserAccessMode();

            //DefaultStorageTypeName = Settings["Raven/StorageTypeName"] ??
            //    "Raven.Storage.Esent.TransactionalStorage, Raven.Storage.Esent";

            DefaultStorageTypeName = Settings["Raven/StorageTypeName"] ??
                "Raven.Storage.Managed.TransactionalStorage, Raven.Storage.Managed";
        }

        public NameValueCollection Settings { get; set; }

        public string DefaultStorageTypeName { get; set; }

        public string ServerUrl
        {
            get { return "http://" + (HostName ?? Environment.MachineName) + ":" + Port + VirtualDirectory; }
        }

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

                pluginsDirectory = value;

                // add new one
                if (Directory.Exists(pluginsDirectory))
                {
                    Catalog.Catalogs.Add(new DirectoryCatalog(pluginsDirectory));
                }
            }
        }

        public TransactionMode TransactionMode { get; set; }

        public string DataDirectory
        {
            get { return dataDirectory; }
            set { dataDirectory = value.ToFullPath(); }
        }

        /// <summary>
        ///   null to accept any hostname or address
        /// </summary>
        public string HostName { get; set; }

        public int Port { get; set; }
        public string WebDir { get; set; }
        public string AccessControlAllowOrigin { get; set; }
        public AnonymousUserAccessMode AnonymousUserAccessMode { get; set; }

        public string VirtualDirectory { get; set; }

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

        public bool RunInMemory { get; set; }

        public bool HttpCompression { get; set; }

        public int MaxPageSize { get; set; }


        public int TempIndexPromotionThreshold { get; set; }
        public int TempIndexPromotionMinimumQueryCount { get; set; }
        public TimeSpan TempIndexCleanupPeriod { get; set; }
        public TimeSpan TempIndexCleanupThreshold { get; set; }

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

        public void LoadLoggingSettings()
        {
            XmlConfigurator.ConfigureAndWatch(
                new FileInfo(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "log4net.config")));
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
            var type =
                Type.GetType(storageEngine.Split(',').First()) ?? // first try to find the merged one
                    Type.GetType(storageEngine); // then try full type name

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
    }
}