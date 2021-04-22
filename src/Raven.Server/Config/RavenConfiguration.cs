using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Configuration;

#if !RVN
using Microsoft.Extensions.Configuration.CommandLine;
using Microsoft.Extensions.Configuration.Memory;
using Raven.Client.Documents.Conventions;
#endif

using Raven.Client.Extensions;
using Raven.Server.Commercial;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Voron.Util.Settings;

namespace Raven.Server.Config
{
    public class RavenConfiguration
    {
#if !RVN
        internal static readonly RavenConfiguration Default = new RavenConfiguration("__default", ResourceType.Server);

        private readonly string _customConfigPath;

        private readonly IConfigurationBuilder _configBuilder;

        public bool Initialized { get; private set; }
#endif

        public CoreConfiguration Core { get; }

#if !RVN
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

        public EmbeddedConfiguration Embedded { get; }

        public MemoryConfiguration Memory { get; }

        public StudioConfiguration Studio { get; }

        public DatabaseConfiguration Databases { get; }

        public PerformanceHintsConfiguration PerformanceHints { get; }

        public LicenseConfiguration Licensing { get; }

        public TombstoneConfiguration Tombstones { get; }

        public SubscriptionsConfiguration Subscriptions { get; }

        public TransactionMergerConfiguration TransactionMergerConfiguration { get; }

        public NotificationsConfiguration Notifications { get; }

        public UpdatesConfiguration Updates { get; }

        public MigrationConfiguration Migration { get; }

        internal IConfigurationRoot ServerWideSettings { get; set; }

        internal IConfigurationRoot Settings { get; set; }

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
            Embedded = new EmbeddedConfiguration();
            Databases = new DatabaseConfiguration(Storage.ForceUsing32BitsPager);
            Memory = new MemoryConfiguration();
            Studio = new StudioConfiguration();
            Licensing = new LicenseConfiguration();
            Tombstones = new TombstoneConfiguration();
            Subscriptions = new SubscriptionsConfiguration();
            TransactionMergerConfiguration = new TransactionMergerConfiguration(Storage.ForceUsing32BitsPager);
            Notifications = new NotificationsConfiguration();
            Updates = new UpdatesConfiguration();
            Migration = new MigrationConfiguration();
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
            var settingsNames = Settings.AsEnumerable().Select(pair => pair.Key).ToHashSet(StringComparer.OrdinalIgnoreCase);
            var serverWideSettingsNames = ServerWideSettings?.AsEnumerable().Select(pair => pair.Key).ToHashSet(StringComparer.OrdinalIgnoreCase);

            Http.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Embedded.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Server.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Core.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Replication.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Cluster.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Etl.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Queries.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Patching.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Logs.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Memory.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Storage.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Security.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Backup.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Indexing.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Monitoring.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Studio.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Databases.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            PerformanceHints.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Licensing.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Tombstones.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Subscriptions.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            TransactionMergerConfiguration.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Notifications.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Updates.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);
            Migration.Initialize(Settings, settingsNames, ServerWideSettings, serverWideSettingsNames, ResourceType, ResourceName);

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

        internal static readonly Lazy<HashSet<ConfigurationEntryMetadata>> AllConfigurationEntries = new Lazy<HashSet<ConfigurationEntryMetadata>>(() =>
        {
            var results = new HashSet<ConfigurationEntryMetadata>();

            var type = typeof(RavenConfiguration);
            foreach (var configurationCategoryProperty in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                var propertyType = configurationCategoryProperty.PropertyType;
                if (propertyType.IsSubclassOf(typeof(ConfigurationCategory)) == false)
                    continue;

                foreach (var configurationProperty in propertyType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
                {
                    if (configurationProperty.GetCustomAttributes<ConfigurationEntryAttribute>(inherit: true).Any() == false)
                        continue;

                    results.Add(new ConfigurationEntryMetadata(configurationCategoryProperty, configurationProperty));
                }
            }

            return results;
        });

        public static bool ContainsKey(string key)
        {
            return AllConfigurationEntries.Value.Any(x => x.IsMatch(key));
        }
#endif

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

#if !RVN
        public static object GetValue<T>(Expression<Func<RavenConfiguration, T>> getKey, RavenConfiguration serverConfiguration, Dictionary<string, string> settings)
        {
            TimeUnitAttribute timeUnit = null;

            var property = (PropertyInfo)getKey.ToProperty();
            if (property.PropertyType == typeof(TimeSetting) ||
                property.PropertyType == typeof(TimeSetting?))
            {
                timeUnit = property.GetCustomAttribute<TimeUnitAttribute>();
                Debug.Assert(timeUnit != null);
            }

            object value = null;
            foreach (var entry in property
                .GetCustomAttributes<ConfigurationEntryAttribute>()
                .OrderBy(x => x.Order))
            {
                if (settings.TryGetValue(entry.Key, out var valueAsString) == false)
                    value = serverConfiguration.GetSetting(entry.Key);

                if (valueAsString != null)
                {
                    value = valueAsString;
                    break;
                }
            }

            if (value == null)
                value = GetDefaultValue(getKey);

            if (value == null)
                return null;

            if (timeUnit != null)
                return new TimeSetting(Convert.ToInt64(value), timeUnit.Unit);

            throw new NotSupportedException("Cannot get value of a property of type: " + property.PropertyType.Name);
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
                if (configurationProperty.PropertyType.IsSubclassOf(typeof(ConfigurationCategory)) == false)
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
                        // if there is no 'path' directory, we are going to create a directory with a similar name, in order to avoid deleting a directory in use afterwards,
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

        public static string EnvironmentVariableLicenseString { private set; get; }

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

                    if (key.StartsWith(Prefix1, StringComparison.OrdinalIgnoreCase) == false &&
                        key.StartsWith(Prefix2, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    var originalKey = key;

                    key = key
                        .Substring(Prefix1.Length)
                        .Replace("_", ".");

                    if (key.Equals(LicenseHelper.LicenseStringConfigurationName, StringComparison.OrdinalIgnoreCase))
                    {
                        EnvironmentVariableLicenseString = originalKey;
                    }

                    Data[key] = env.Value as string;
                }
            }
        }

        public bool DoesKeyExistInSettings(string keyName, bool serverWide = false)
        {
            IConfiguration cfg = serverWide ? ServerWideSettings : Settings;

            if (cfg == null || keyName == null)
                return false;

            foreach (var kvp in cfg.AsEnumerable())
            {
                if (string.Equals(kvp.Key, keyName, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }
#endif
    }
}
