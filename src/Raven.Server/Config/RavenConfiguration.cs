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

        public TenantConfiguration Tenants { get; }

        public LicenseConfiguration Licensing { get; }

        public QuotasBundleConfiguration Quotas { get; }

        protected NameValueCollection Settings { get; set; }

        public RavenConfiguration()
        {
            Settings = new NameValueCollection(StringComparer.OrdinalIgnoreCase);

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
            Tenants = new TenantConfiguration();
            Licensing = new LicenseConfiguration();
            Quotas = new QuotasBundleConfiguration();
        }

        public static string GetKey<T>(Expression<Func<RavenConfiguration, T>> getKey)
        {
            var prop = getKey.ToProperty();
            return prop.GetCustomAttributes<ConfigurationEntryAttribute>().OrderBy(x => x.Order).First().Key;
        }
    }
}