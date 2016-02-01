using System;
using System.Collections.Specialized;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Extensions;

namespace Raven.Server.Config
{
    public class RavenConfiguration
    {
        private bool initialized = false;
        private bool allowChanges = false;

        public CoreConfiguration Core { get; }

        public ReplicationConfiguration Replication { get; }

        public StorageConfiguration Storage { get; }

        public EncryptionConfiguration Encryption { get; }

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

        protected NameValueCollection Settings { get; set; }

        public RavenConfiguration()
        {
            Settings = new NameValueCollection(StringComparer.OrdinalIgnoreCase);

            Core = new CoreConfiguration();

            Replication = new ReplicationConfiguration();
            Storage = new StorageConfiguration();
            Encryption = new EncryptionConfiguration();
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
            Monitoring.Initialize(Settings);
            Expiration.Initialize(Settings);
            Versioning.Initialize(Settings);
            Studio.Initialize(Settings);
            Databases.Initialize(Settings);
            Licensing.Initialize(Settings);
            Quotas.Initialize(Settings);

            PostInit();

            initialized = true;

            return this;
        }

        public void PostInit()
        {
            
        }

        public void CopyParentSettings(RavenConfiguration serverConfiguration)
        {
            Encryption.UseSsl = serverConfiguration.Encryption.UseSsl;
            Encryption.UseFips = serverConfiguration.Encryption.UseFips;

            Storage.AllowOn32Bits = serverConfiguration.Storage.AllowOn32Bits;
        }

        public void SetSetting(string key, string value)
        {
            if (initialized && allowChanges == false)
                throw new InvalidOperationException("Configuration already initialized. You cannot specify an already initialized setting.");

            Settings[key] = value;
        }

        public string GetSetting(string key)
        {
            return Settings[key];
        }

        public static string GetKey<T>(Expression<Func<RavenConfiguration, T>> getKey)
        {
            var prop = getKey.ToProperty();
            return prop.GetCustomAttributes<ConfigurationEntryAttribute>().OrderBy(x => x.Order).First().Key;
        }

        public static RavenConfiguration CreateFrom(RavenConfiguration parent)
        {
            var result = new RavenConfiguration
            {
                Settings = new NameValueCollection(parent.Settings)
            };

            result.Settings[GetKey(x => x.Core.RunInMemory)] = parent.Core.RunInMemory.ToString();

            return result;
        }
    }
}