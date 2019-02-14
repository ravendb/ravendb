using System;
using System.IO;
using System.Reflection;
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public static class DatabaseHelper
    {
        private static readonly Lazy<string[]> ServerWideOnlyConfigurationKeys = new Lazy<string[]>(GetServerWideOnlyConfigurationKeys);

        public static string[] GetServerWideOnlyConfigurationKeys()
        {
            var keys = new string[0];
            foreach (var configurationProperty in typeof(RavenConfiguration).GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                if (configurationProperty.PropertyType.GetTypeInfo().IsSubclassOf(typeof(ConfigurationCategory)) == false)
                    continue;

                foreach (var categoryProperty in configurationProperty.PropertyType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
                {
                    foreach (var configurationEntryAttribute in categoryProperty.GetCustomAttributes<ConfigurationEntryAttribute>())
                    {
                        if (configurationEntryAttribute.Scope == ConfigurationEntryScope.ServerWideOrPerDatabase)
                            continue;

                        Array.Resize(ref keys, keys.Length + 1);
                        keys[keys.Length - 1] = configurationEntryAttribute.Key;
                    }
                }
            }

            return keys;
        }

        public static bool CheckExistingDatabaseName(BlittableJsonReaderObject database, string id, string dbId, string etag, out string errorMessage)
        {
            var isExistingDatabase = database != null;
            if (isExistingDatabase && etag == null)
            {
                errorMessage = $"Database with the name '{id}' already exists";
                return false;
            }
            if (!isExistingDatabase && etag != null)
            {
                errorMessage = $"Database with the name '{id}' doesn't exist";
                return false;
            }

            errorMessage = null;
            return true;
        }

        public static void DeleteDatabaseFiles(RavenConfiguration configuration)
        {
            if (configuration.Core.RunInMemory)
                return;

            IOExtensions.DeleteDirectory(configuration.Core.DataDirectory.FullPath);

            if (configuration.Storage.TempPath != null)
                IOExtensions.DeleteDirectory(configuration.Storage.TempPath.FullPath);

            if (configuration.Indexing.StoragePath != null)
                IOExtensions.DeleteDirectory(configuration.Indexing.StoragePath.FullPath);

            if (configuration.Indexing.TempPath != null)
                IOExtensions.DeleteDirectory(configuration.Indexing.TempPath.FullPath);
        }

        public static void Validate(string name, DatabaseRecord record, RavenConfiguration serverConfiguration)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));
            if (record == null)
                throw new ArgumentNullException(nameof(record));

            if (record.DatabaseName != null && string.Equals(name, record.DatabaseName, StringComparison.OrdinalIgnoreCase) == false)
                throw new InvalidOperationException("Name does not match.");

            if (record.Settings != null &&
                record.Settings.TryGetValue(RavenConfiguration.GetKey(x => x.Core.DataDirectory), out var dataDir) &&
                dataDir != null)
            {
                var databasePath = new PathSetting(dataDir, serverConfiguration.Core.DataDirectory.FullPath);

                if (databasePath.Equals(serverConfiguration.Core.DataDirectory))
                    throw new InvalidOperationException(
                        $"Forbidden data directory path for database '{name}': '{dataDir}'. This is the root path that RavenDB server uses to store data.");
                if (Path.GetPathRoot(databasePath.FullPath) == databasePath.FullPath)
                    throw new InvalidOperationException(
                        $"Forbidden data directory path for database '{name}': '{dataDir}'. You cannot use the root directory of the drive as the database path.");
            }

            foreach (var key in ServerWideOnlyConfigurationKeys.Value)
            {
                if (record.Settings != null && record.Settings.TryGetValue(key, out _))
                    throw new InvalidOperationException($"Detected '{key}' key in {nameof(DatabaseRecord.Settings)}. This is a server-wide configuration key and can only be set at server level.");
            }
        }
    }
}
