using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Raven.Abstractions.Extensions;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Utils;
using ExpressionExtensions = Raven.Server.Extensions.ExpressionExtensions;

namespace Raven.Server.Config
{
    public class RavenConfiguration
    {
        public bool Initialized { get; private set; }
        private bool allowChanges = false;

        private readonly IConfigurationBuilder _configBuilder;

        public CoreConfiguration Core { get; }

        public ReplicationConfiguration Replication { get; }

        public StorageConfiguration Storage { get; }

        public EncryptionConfiguration Encryption { get; }

        public IndexingConfiguration Indexing { get; set; }

        public MonitoringConfiguration Monitoring { get; }

        public WebSocketsConfiguration WebSockets { get; set; }

        public QueryConfiguration Queries { get; }

        public PatchingConfiguration Patching { get; }

        public BulkInsertConfiguration BulkInsert { get; }

        public ServerConfiguration Server { get; }

        public MemoryConfiguration Memory { get; }

        public ExpirationBundleConfiguration Expiration { get; }

        public VersioningBundleConfiguration Versioning { get; }

        public StudioConfiguration Studio { get; }

        public DatabaseConfiguration Databases { get; }

        public LicenseConfiguration Licensing { get; }

        public QuotasBundleConfiguration Quotas { get; }

        protected IConfigurationRoot Settings { get; set; }

        public RavenConfiguration()
        {
            _configBuilder = new ConfigurationBuilder()
                .AddJsonFile("settings.json", optional: true)
                .AddEnvironmentVariables(prefix: "RAVEN_");
            Settings = _configBuilder.Build();
            Core = new CoreConfiguration();

            Replication = new ReplicationConfiguration();
            Storage = new StorageConfiguration();
            Encryption = new EncryptionConfiguration();
            Indexing = new IndexingConfiguration(() => Core.RunInMemory, () => Core.DataDirectory);
            WebSockets = new WebSocketsConfiguration();
            Monitoring = new MonitoringConfiguration();
            Queries = new QueryConfiguration();
            Patching = new PatchingConfiguration();
            BulkInsert = new BulkInsertConfiguration();
            Server = new ServerConfiguration();
            Memory = new MemoryConfiguration();
            Expiration = new ExpirationBundleConfiguration();
            Versioning = new VersioningBundleConfiguration();
            Studio = new StudioConfiguration();
            Databases = new DatabaseConfiguration();
            Licensing = new LicenseConfiguration();
            Quotas = new QuotasBundleConfiguration();
        }

        public string DatabaseName { get; set; }
        public RavenWebHostConfiguration WebHostConfig { get; private set; }

        public RavenConfiguration Initialize()
        {
            Core.Initialize(Settings);
            Replication.Initialize(Settings);
            Queries.Initialize(Settings);
            Patching.Initialize(Settings);
            BulkInsert.Initialize(Settings);
            Server.Initialize(Settings);
            Memory.Initialize(Settings);
            Storage.Initialize(Settings);
            Encryption.Initialize(Settings);
            Indexing.Initialize(Settings);
            Monitoring.Initialize(Settings);
            Expiration.Initialize(Settings);
            Versioning.Initialize(Settings);
            Studio.Initialize(Settings);
            Databases.Initialize(Settings);
            Licensing.Initialize(Settings);
            Quotas.Initialize(Settings);

            PostInit();

            Initialized = true;

            return this;
        }

        public void PostInit()
        {
            WebHostConfig = new RavenWebHostConfiguration(this);
        }

        public void CopyParentSettings(RavenConfiguration serverConfiguration)
        {
            Encryption.UseSsl = serverConfiguration.Encryption.UseSsl;
            Encryption.UseFips = serverConfiguration.Encryption.UseFips;

            Storage.AllowOn32Bits = serverConfiguration.Storage.AllowOn32Bits;
        }

        public void SetSetting(string key, string value)
        {
            if (Initialized && allowChanges == false)
                throw new InvalidOperationException("Configuration already initialized. You cannot specify an already initialized setting.");

            Settings[key] = value;
        }

        public string GetSetting(string key)
        {
            return Settings[key];
        }

        public static string GetKey<T>(Expression<Func<RavenConfiguration, T>> getKey)
        {
            var prop = ExpressionExtensions.ToProperty(getKey);
            return prop.GetCustomAttributes<ConfigurationEntryAttribute>().OrderBy(x => x.Order).First().Key;
        }

        public static RavenConfiguration CreateFrom(RavenConfiguration parent)
        {
            var result = new RavenConfiguration
            {
                Settings = parent._configBuilder.Build()
            };

            result.Settings[GetKey(x => x.Core.RunInMemory)] = parent.Core.RunInMemory.ToString();

            return result;
        }

        public void AddCommandLine(string[] args)
        {
            _configBuilder.AddCommandLine(args);
            Settings = _configBuilder.Build();
        }
    }
}