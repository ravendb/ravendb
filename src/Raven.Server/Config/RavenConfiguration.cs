using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Memory;
using Raven.Client.Documents.Conventions;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Maintenance;
using ExpressionExtensions = Raven.Server.Extensions.ExpressionExtensions;
using Sparrow.Platform;

namespace Raven.Server.Config
{
    public class RavenConfiguration
    {
        private readonly IConfigurationBuilder _configBuilder;

        public bool Initialized { get; private set; }

        public CoreConfiguration Core { get; }

        public EtlConfiguration Etl { get; }

        public ReplicationConfiguration Replication { get; }

        public ClusterConfiguration Cluster { get; }

        public StorageConfiguration Storage { get; }

        public SecurityConfiguration Security { get; }

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

        public RavenConfiguration(string resoureName, ResourceType resourceType, string customConfigPath = null)
        {
            ResourceName = resoureName;
            ResourceType = resourceType;

            _configBuilder = new ConfigurationBuilder();
            AddEnvironmentVariables();
            AddJsonConfigurationVariables(customConfigPath);

            Settings = _configBuilder.Build();
            Core = new CoreConfiguration();

            Replication = new ReplicationConfiguration();
            Cluster = new ClusterConfiguration();
            Etl = new EtlConfiguration();
            Storage = new StorageConfiguration();
            Security = new SecurityConfiguration();
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
            Memory = new MemoryConfiguration();
            Expiration = new ExpirationBundleConfiguration();
            Studio = new StudioConfiguration();
            Licensing = new LicenseConfiguration();
            Quotas = new QuotasBundleConfiguration();
            Tombstones = new TombstoneConfiguration();
        }

        private void AddJsonConfigurationVariables(string customConfigPath = null)
        {
            if (string.IsNullOrEmpty(customConfigPath) == false && File.Exists(customConfigPath))
            {
                _configBuilder.AddJsonFile(customConfigPath);
                return;
            }

            var platformPostfix = "windows";
            if (PlatformDetails.RunningOnPosix)
                platformPostfix = "posix";

            _configBuilder.AddJsonFile($"settings_{platformPostfix}.json", optional: true);
            _configBuilder.AddJsonFile($"settings.json", optional: true);
        }

        private void AddEnvironmentVariables()
        {
            foreach (DictionaryEntry de in Environment.GetEnvironmentVariables())
            {
                var s = de.Key as string;
                if (s == null)
                    continue;
                if (s.StartsWith("RAVEN_") == false)
                    continue;

                _configBuilder.Properties[s.Replace("RAVEN_", "Raven/")] = de.Value;
            }
        }

        public DebugLoggingConfiguration DebugLog { get; set; }

        public string ResourceName { get; }

        public ResourceType ResourceType { get; }

        public RavenConfiguration Initialize()
        {
            Server.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Core.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Replication.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Cluster.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Etl.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Queries.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Patching.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            DebugLog.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            BulkInsert.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Memory.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Storage.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Security.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
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
            CheckDirectoryPermissions();
        }

        public void CopyParentSettings(RavenConfiguration serverConfiguration)
        {
            Security.CertificatePassword = serverConfiguration.Security.CertificatePassword;
            Security.CertificateFilePath = serverConfiguration.Security.CertificateFilePath;

            Storage.ForceUsing32BitsPager = serverConfiguration.Storage.ForceUsing32BitsPager;

            Queries.MaxClauseCount = serverConfiguration.Queries.MaxClauseCount;
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
            var dataDirectory = parent.Core.DataDirectory;
            var dataDirectoryPath = dataDirectory != null ? parent.Core.DataDirectory.Combine(Inflector.Pluralize(type.ToString())).Combine(name).ToFullPath() :
                      GenerateDefaultDataDirectory(parent.Core.GetDefaultValue<CoreConfiguration>(v => v.DataDirectory).ToString(),type,name);

            var result = new RavenConfiguration(name, type)
            {
                ServerWideSettings = parent.Settings,
                Settings = new ConfigurationRoot(new List<IConfigurationProvider> { new MemoryConfigurationProvider(new MemoryConfigurationSource()) })
                {
                    [GetKey(x => x.Core.RunInMemory)] = parent.Core.RunInMemory.ToString(),
                    [GetKey(x => x.Core.DataDirectory)] = dataDirectoryPath
                }
            };

            return result;
        }

        private static string GenerateDefaultDataDirectory(string template, ResourceType type,string name)
        {
            return template.Replace("{pluralizedResourceType}", Inflector.Pluralize(type.ToString()))
                           .Replace("{name}", name);
        }

        public void AddCommandLine(string[] args)
        {
            _configBuilder.AddCommandLine(args);
            Settings = _configBuilder.Build();
        }

        private void CheckDirectoryPermissions()
        {
            if (Core.RunInMemory)
                return;

            Dictionary<string, KeyValuePair<string, string>> results = null;
            foreach (var configurationProperty in typeof(RavenConfiguration).GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                if (configurationProperty.PropertyType.GetTypeInfo().IsSubclassOf(typeof(ConfigurationCategory)) == false)
                    continue;

                var categoryValue = configurationProperty.GetValue(this);

                foreach (var categoryProperty in configurationProperty.PropertyType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
                {
                    if (categoryProperty.PropertyType != typeof(PathSetting))
                        continue;

                    var pathSettingValue = categoryProperty.GetValue(categoryValue) as PathSetting;
                    if (pathSettingValue == null)
                        continue;

                    var fileName = Guid.NewGuid().ToString("N");
                    var path = pathSettingValue.ToFullPath();
                    var fullPath = Path.Combine(path, fileName);
                    var configurationKey = categoryProperty.GetCustomAttributes<ConfigurationEntryAttribute>()
                        .OrderBy(x => x.Order)
                        .First()
                        .Key;

                    try
                    {
                        if (Directory.Exists(path) == false)
                            Directory.CreateDirectory(path);

                        File.WriteAllText(fullPath, string.Empty);
                        File.Delete(fullPath);
                    }
                    catch (Exception e)
                    {
                        if (results == null)
                            results = new Dictionary<string, KeyValuePair<string, string>>();

                        results[configurationKey] = new KeyValuePair<string, string>(path, e.Message);
                    }
                }
            }

            if (results == null || results.Count == 0)
                return;

            var sb = new StringBuilder("Could not access some of the specified paths. Please check if you have sufficient privileges to access following paths:");
            sb.Append(Environment.NewLine);

            foreach (var result in results)
                sb.AppendLine($"Key: '{result.Key}' Path: '{result.Value.Key}' Error: '{result.Value.Value}'");

            throw new InvalidOperationException(sb.ToString());
        }
    }
}