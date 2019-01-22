using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.CommandLine;
using Microsoft.Extensions.Configuration.Memory;
using Raven.Client.Documents.Conventions;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Voron.Util.Settings;

namespace Raven.Server.Config
{
    public class RavenConfiguration
    {
        private readonly string _customConfigPath;

        private readonly IConfigurationBuilder _configBuilder;

        public bool Initialized { get; private set; }

        public CoreConfiguration Core { get; }

        public HttpConfiguration Http { get; }

        public EtlConfiguration Etl { get; }

        public ReplicationConfiguration Replication { get; }

        public ClusterConfiguration Cluster { get; }

        public StorageConfiguration Storage { get; }

        public SecurityConfiguration Security { get; }

        public BackupConfiguration Backup { get; }

        public IndexingConfiguration Indexing { get; set; }

        public MonitoringConfiguration Monitoring { get; }

        public QueryConfiguration Queries { get; }

        public PatchingConfiguration Patching { get; }

        public ServerConfiguration Server { get; }

        public TestingConfiguration Testing { get; }

        public MemoryConfiguration Memory { get; }

        public StudioConfiguration Studio { get; }

        public DatabaseConfiguration Databases { get; }

        public PerformanceHintsConfiguration PerformanceHints { get; }

        public LicenseConfiguration Licensing { get; }

        public TombstoneConfiguration Tombstones { get; }

        public SubscriptionConfiguration Subscriptions { get; }

        public TransactionMergerConfiguration TransactionMergerConfiguration { get; }

        internal IConfigurationRoot ServerWideSettings { get; set; }

        protected IConfigurationRoot Settings { get; set; }

        internal string ConfigPath => _customConfigPath
                       ?? Path.Combine(AppContext.BaseDirectory, "settings.json");

        internal CommandLineConfigurationSource CommandLineSettings =>
            _configBuilder.Sources.OfType<CommandLineConfigurationSource>().FirstOrDefault();

        private RavenConfiguration(string resourceName, ResourceType resourceType, string customConfigPath = null)
        {
            ResourceName = resourceName;
            ResourceType = resourceType;
            _customConfigPath = customConfigPath;
            PathSettingBase<string>.ValidatePath(_customConfigPath);
            _configBuilder = new ConfigurationBuilder();
            AddEnvironmentVariables();
            AddJsonConfigurationVariables(customConfigPath);

            Settings = _configBuilder.Build();

            Core = new CoreConfiguration();

            Http = new HttpConfiguration();
            Replication = new ReplicationConfiguration();
            Cluster = new ClusterConfiguration();
            Etl = new EtlConfiguration();
            Storage = new StorageConfiguration();
            Security = new SecurityConfiguration();
            Backup = new BackupConfiguration();
            PerformanceHints = new PerformanceHintsConfiguration();
            Indexing = new IndexingConfiguration(this);
            Monitoring = new MonitoringConfiguration();
            Queries = new QueryConfiguration();
            Patching = new PatchingConfiguration();
            Logs = new LogsConfiguration();
            Server = new ServerConfiguration();
            Testing = new TestingConfiguration();
            Databases = new DatabaseConfiguration();
            Memory = new MemoryConfiguration();
            Studio = new StudioConfiguration();
            Licensing = new LicenseConfiguration();
            Tombstones = new TombstoneConfiguration();
            Subscriptions = new SubscriptionConfiguration();
            TransactionMergerConfiguration = new TransactionMergerConfiguration(Storage.ForceUsing32BitsPager);
        }

        private void AddJsonConfigurationVariables(string customConfigPath = null)
        {
            if (string.IsNullOrEmpty(customConfigPath) == false)
            {
                if (File.Exists(customConfigPath) == false)
                    throw new FileNotFoundException("Custom configuration file has not been found.", customConfigPath);

                _configBuilder.AddJsonFile(customConfigPath, optional: true);
            }
            else
            {
                _configBuilder.AddJsonFile("settings.json", optional: true);
            }
        }

        private void AddEnvironmentVariables()
        {
            _configBuilder.Add(new EnvironmentVariablesConfigurationSource());
        }

        public LogsConfiguration Logs { get; set; }

        public string ResourceName { get; }

        public ResourceType ResourceType { get; }

        public RavenConfiguration Initialize()
        {
            Http.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Testing.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Server.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Core.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Replication.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Cluster.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Etl.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Queries.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Patching.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Logs.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Memory.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Storage.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Security.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Backup.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Indexing.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Monitoring.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Studio.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Databases.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            PerformanceHints.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Licensing.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Tombstones.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            Subscriptions.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);
            TransactionMergerConfiguration.Initialize(Settings, ServerWideSettings, ResourceType, ResourceName);

            PostInit();

            Initialized = true;

            return this;
        }

        public void PostInit()
        {
            if (ResourceType != ResourceType.Server)
                return;

            SecurityConfiguration.Validate(this);
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

        private static readonly Lazy<HashSet<string>> AllConfigurationKeys = new Lazy<HashSet<string>>(() =>
        {
            var results = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var type = typeof(RavenConfiguration);
            foreach (var configurationProperty in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                var propertyType = configurationProperty.PropertyType;
                if (propertyType.GetTypeInfo().IsSubclassOf(typeof(ConfigurationCategory)) == false)
                    continue;

                foreach (var property in propertyType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
                {
                    var attribute = property.GetCustomAttribute<ConfigurationEntryAttribute>();
                    if (attribute == null)
                        continue;

                    results.Add(attribute.Key);
                }
            }

            return results;
        });
        

        public static bool ContainsKey(string key)
        {
            return AllConfigurationKeys.Value.Contains(key, StringComparer.OrdinalIgnoreCase);
        }

        public static string GetKey<T>(Expression<Func<RavenConfiguration, T>> getKey)
        {
            var property = getKey.ToProperty();
            var configurationEntryAttribute = property
                .GetCustomAttributes<ConfigurationEntryAttribute>()
                .OrderBy(x => x.Order)
                .FirstOrDefault();

            if (configurationEntryAttribute == null)
                throw new InvalidOperationException($"Property '{property.Name}' does not contain '{nameof(ConfigurationEntryAttribute)}'. Please make sure that this is a valid configuration property.");

            return configurationEntryAttribute.Key;
        }

        public static object GetDefaultValue<T>(Expression<Func<RavenConfiguration, T>> getKey)
        {
            var prop = getKey.ToProperty();
            return prop.GetCustomAttributes<DefaultValueAttribute>().FirstOrDefault()?.Value;
        }

        public static string GetDataDirectoryPath(CoreConfiguration coreConfiguration, string name, ResourceType type)
        {
            var dataDirectory = coreConfiguration.DataDirectory;
            var dataDirectoryPath = dataDirectory != null ? coreConfiguration.DataDirectory.Combine(Inflector.Pluralize(type.ToString())).Combine(name).ToFullPath() :
                GenerateDefaultDataDirectory(coreConfiguration.GetDefaultValue<CoreConfiguration>(v => v.DataDirectory).ToString(), type, name);
            return dataDirectoryPath;
        }

        public static RavenConfiguration CreateForServer(string name, string customConfigPath = null)
        {
            return new RavenConfiguration(name, ResourceType.Server, customConfigPath);
        }

        public static RavenConfiguration CreateForDatabase(RavenConfiguration parent, string name)
        {
            var dataDirectoryPath = GetDataDirectoryPath(parent.Core, name, ResourceType.Database);

            var result = new RavenConfiguration(name, ResourceType.Database)
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

        /// <summary>
        /// This method should only be used for testing purposes
        /// </summary>
        internal static RavenConfiguration CreateForTesting(string name, ResourceType resourceType, string customConfigPath = null)
        {
            return new RavenConfiguration(name, resourceType, customConfigPath);
        }

        private static string GenerateDefaultDataDirectory(string template, ResourceType type, string name)
        {
            return template.Replace("{pluralizedResourceType}", Inflector.Pluralize(type.ToString()))
                           .Replace("{name}", name);
        }

        public void AddCommandLine(string[] args)
        {
            _configBuilder.AddCommandLine(args);
            Settings = _configBuilder.Build();
        }
        
        private static int _pathCounter = 0;

        public void CheckDirectoryPermissions()
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

                    var readOnly = categoryProperty.GetCustomAttribute<ReadOnlyPathAttribute>();
                    if (readOnly != null)
                        continue;

                    var fileName = Guid.NewGuid().ToString("N");
                    
                    
                    var path = pathSettingValue.ToFullPath();
                    var fullPath = Path.Combine(path, fileName);

                    var configEntry = categoryProperty.GetCustomAttributes<ConfigurationEntryAttribute>()
                        .OrderBy(x => x.Order)
                        .First();

                    if (configEntry.Scope == ConfigurationEntryScope.ServerWideOnly &&
                        ResourceType == ResourceType.Database)
                        continue;

                    var configurationKey = configEntry.Key;

                    string createdDirectory = null;
                    try
                    {
                        // if there is no 'path' directory, we are going to create a directory with a similiar name, in order to avoid deleting a directory in use afterwards,
                        // and write a sample file inside it, in order to check write permissions.
                        if (Directory.Exists(path) == false)
                        {
                            var curPathCounterVal = Interlocked.Increment(ref _pathCounter);
                            // test that we can create the directory, but
                            // not actually create it
                            createdDirectory = path + "$" + curPathCounterVal.ToString();
                            Directory.CreateDirectory(createdDirectory);
                            var createdFile = Path.Combine(createdDirectory, "test.file");
                            File.WriteAllText(createdFile, string.Empty);
                            File.Delete(createdFile);
                        }
                        // in case there is a 'path' directory, we are going to try and write to it some file, in order to check write permissions
                        else
                        {
                            File.WriteAllText(fullPath, string.Empty);
                            File.Delete(fullPath);
                        }
                    }
                    catch (Exception e)
                    {
                        if (results == null)
                            results = new Dictionary<string, KeyValuePair<string, string>>();

                        var errorousDirPath = createdDirectory ?? path;
                        results[configurationKey] = new KeyValuePair<string, string>(errorousDirPath, e.Message);
                    }
                    finally
                    {
                        if (createdDirectory != null)
                        {
                            Interlocked.Decrement(ref _pathCounter);
                            IOExtensions.DeleteDirectory(createdDirectory);
                        }
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

        private class EnvironmentVariablesConfigurationSource : ConfigurationProvider, IConfigurationSource
        {
            private const string Prefix1 = "RAVEN.";

            private const string Prefix2 = "RAVEN_";

            public IConfigurationProvider Build(IConfigurationBuilder builder)
            {
                return this;
            }

            public override void Load()
            {
                Data = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                var envs = Environment.GetEnvironmentVariables().Cast<DictionaryEntry>();
                foreach (var env in envs)
                {
                    var key = env.Key as string;
                    if (key == null)
                        continue;

                    if (key.StartsWith(Prefix1, StringComparison.OrdinalIgnoreCase) == false && key.StartsWith(Prefix2, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    key = key
                        .Substring(Prefix1.Length)
                        .Replace("_", ".");

                    Data[key] = env.Value as string;
                }
            }
        }
    }
}
