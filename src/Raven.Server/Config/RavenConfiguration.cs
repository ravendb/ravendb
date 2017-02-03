using System;
using System.Collections;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Extensions.Configuration;

using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using ExpressionExtensions = Raven.Server.Extensions.ExpressionExtensions;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Server.Config
{
    public class RavenConfiguration
    {
        private readonly IConfigurationBuilder _configBuilder;

        public bool Initialized { get; private set; }

        public CoreConfiguration Core { get; }

        public SqlReplicationConfiguration SqlReplication { get; }

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

        public StudioConfiguration Studio { get; }

        public DatabaseConfiguration Databases { get; }

        public PerformanceHintsConfiguration PerformanceHints { get; }

        public LicenseConfiguration Licensing { get; }

        public QuotasBundleConfiguration Quotas { get; }

        public TombstoneConfiguration Tombstones { get; }

        internal IConfigurationRoot ServerWideSettings { get; set; }

        protected IConfigurationRoot Settings { get; set; }

        internal PathSetting ServerDataDir { get; private set; }

        public RavenConfiguration(string resoureName, ResourceType resourceType)
        {
            ResourceName = resoureName;
            ResourceType = resourceType;

            _configBuilder = new ConfigurationBuilder();
            AddEnvironmentVariables(_configBuilder);
            AddJsonConfigurationVariables();

            Settings = _configBuilder.Build();
            Core = new CoreConfiguration(this);

            Replication = new ReplicationConfiguration();
            SqlReplication = new SqlReplicationConfiguration();
            Storage = new StorageConfiguration();
            Encryption = new EncryptionConfiguration();
            PerformanceHints = new PerformanceHintsConfiguration();
            Indexing = new IndexingConfiguration(this);
            WebSockets = new WebSocketsConfiguration();
            Monitoring = new MonitoringConfiguration();
            Queries = new QueryConfiguration();
            Patching = new PatchingConfiguration();
            DebugLog = new DebugLoggingConfiguration();
            BulkInsert = new BulkInsertConfiguration();
            Server = new ServerConfiguration();
            Databases = new DatabaseConfiguration();
            Memory = new MemoryConfiguration(this);
            Expiration = new ExpirationBundleConfiguration();
            Studio = new StudioConfiguration();
            Licensing = new LicenseConfiguration();
            Quotas = new QuotasBundleConfiguration();
            Tombstones = new TombstoneConfiguration();
        }

        private void AddJsonConfigurationVariables()
        {
            var platformPostfix = "windows";
            if (PlatformDetails.RunningOnPosix)
                platformPostfix = "posix";

            _configBuilder.AddJsonFile($"settings_{platformPostfix}.json", optional: true);
        }

        private static void AddEnvironmentVariables(IConfigurationBuilder configurationBuilder)
        {
            foreach (DictionaryEntry  de in Environment.GetEnvironmentVariables())
            {
                var s = de.Key as string;
                if (s == null)
                    continue;
                if (s.StartsWith("RAVEN_") == false)
                    continue;

                configurationBuilder.Properties[s.Replace("RAVEN_", "Raven/")] = de.Value;
            }
        }

        public DebugLoggingConfiguration DebugLog { get; set; }

        public string ResourceName { get; }

        public ResourceType ResourceType { get; }

        public RavenConfiguration Initialize()
        {
            Core.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Replication.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            SqlReplication.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Queries.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Patching.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            DebugLog.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            BulkInsert.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Server.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Memory.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Storage.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Encryption.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Indexing.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Monitoring.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Expiration.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Studio.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Databases.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            PerformanceHints.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Licensing.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Quotas.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Tombstones.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);

            PostInit();

            Initialized = true;

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
            if (Initialized)
                throw new InvalidOperationException("Configuration already initialized. You cannot specify an already initialized setting.");

            Settings[key] = value;
        }

        public string GetSetting(string key)
        {
            return Settings[key];
        }

        public string GetServerWideSetting(string key)
        {
            return ServerWideSettings?[key];
        }

        public static string GetKey<T>(Expression<Func<RavenConfiguration, T>> getKey)
        {
            var prop = ExpressionExtensions.ToProperty(getKey);
            return prop.GetCustomAttributes<ConfigurationEntryAttribute>().OrderBy(x => x.Order).First().Key;
        }

        public static object GetDefaultValue<T>(Expression<Func<RavenConfiguration, T>> getKey)
        {
            var prop = ExpressionExtensions.ToProperty(getKey);
            return prop.GetCustomAttributes<DefaultValueAttribute>().FirstOrDefault()?.Value;
        }

        public static RavenConfiguration CreateFrom(RavenConfiguration parent, string name, ResourceType type)
        {
            var result = new RavenConfiguration(name, type)
            {
                ServerWideSettings = parent.Settings,
                Settings =
                {
                    [GetKey(x => x.Core.RunInMemory)] = parent.Core.RunInMemory.ToString()
                },
                ServerDataDir = parent.Core.DataDirectory
            };
            
            return result;
        }

        public void AddCommandLine(string[] args)
        {
            _configBuilder.AddCommandLine(args);
            Settings = _configBuilder.Build();
        }
    }
}