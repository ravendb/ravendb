using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Voron;
using Voron.Schema;

namespace Raven.Server.Storage.Schema
{
    public static class SchemaUpgrader
    {
        internal class CurrentVersion
        {
            public const int ServerVersion = 50_000;

            public const int ConfigurationVersion = 50_000;

            public const int DocumentsVersion = 50_002;

            public const int IndexVersion = 50_000;
        }

        private static readonly int[] SkippedDocumentsVersion = { 40_012 };

        public enum StorageType
        {
            Server,
            Configuration,
            Documents,
            Index,
        }

        private class InternalUpgrader
        {
            private readonly StorageType _storageType;
            private readonly ConfigurationStorage _configurationStorage;
            private readonly DocumentsStorage _documentsStorage;
            private readonly ServerStore _serverStore;

            private static readonly Dictionary<StorageType, Dictionary<int, Type>> Updaters = new Dictionary<StorageType, Dictionary<int, Type>>();

            static InternalUpgrader()
            {
                foreach (var type in typeof(SchemaUpgrader)
                    .Assembly
                    .GetTypes())
                {
                    if (type.IsInterface)
                        continue;

                    if (typeof(ISchemaUpdate).IsAssignableFrom(type) == false)
                        continue;

                    var instance = (ISchemaUpdate)Activator.CreateInstance(type);

                    if (Updaters.TryGetValue(instance.StorageType, out var updaters) == false)
                        Updaters[instance.StorageType] = updaters = new Dictionary<int, Type>();

                    updaters.Add(instance.From, instance.GetType());
                }
            }

            internal InternalUpgrader(StorageType storageType, ConfigurationStorage configurationStorage, DocumentsStorage documentsStorage, ServerStore serverStore)
            {
                _storageType = storageType;
                _configurationStorage = configurationStorage;
                _documentsStorage = documentsStorage;
                _serverStore = serverStore;
            }

            internal bool Upgrade(SchemaUpgradeTransactions transactions, int currentVersion, out int versionAfterUpgrade)
            {
                currentVersion = FixCurrentVersion(_storageType, currentVersion);

                switch (_storageType)
                {
                    case StorageType.Server:
                        break;
                    case StorageType.Configuration:
                        break;
                    case StorageType.Documents:
                        if (SkippedDocumentsVersion.Contains(currentVersion))
                            throw new NotSupportedException($"Documents schema upgrade from version {currentVersion} is not supported, use the recovery tool to dump the data and then import it into a new database");
                        break;
                    case StorageType.Index:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(_storageType), _storageType, null);
                }

                versionAfterUpgrade = currentVersion;
                var updater = GetUpdater(_storageType, currentVersion);
                if (updater == null)
                    return false;

                versionAfterUpgrade = updater.To;

                return updater.Update(new UpdateStep(transactions)
                {
                    ConfigurationStorage = _configurationStorage,
                    DocumentsStorage = _documentsStorage,
                });
            }

            private static int FixCurrentVersion(StorageType storageType, int currentVersion)
            {
                switch (storageType)
                {
                    case StorageType.Server:
                        if (currentVersion >= 10 && currentVersion <= 11)
                            currentVersion += 40_000;
                        else if (currentVersion >= 12 && currentVersion <= 19)
                            currentVersion += 42_000;
                        break;
                    case StorageType.Configuration:
                        if (currentVersion >= 10 && currentVersion <= 11)
                            currentVersion += 40_000;
                        break;
                    case StorageType.Documents:
                        if (currentVersion >= 10 && currentVersion <= 15)
                            currentVersion += 40_000;
                        else if (currentVersion == 16)
                            currentVersion += 41_000;
                        else if (currentVersion == 17)
                            currentVersion += 42_000;
                        break;
                    case StorageType.Index:
                        if (currentVersion >= 10 && currentVersion <= 12)
                            currentVersion += 40_000;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(_storageType), storageType, null);
                }

                return currentVersion;
            }

            private static ISchemaUpdate GetUpdater(StorageType storageType, int currentVersion)
            {
                if (Updaters.TryGetValue(storageType, out var updaters) == false)
                    return null;

                if (updaters.TryGetValue(currentVersion, out var updater) == false)
                    return null;

                return (ISchemaUpdate)Activator.CreateInstance(updater);
            }
        }

        public static UpgraderDelegate Upgrader(StorageType storageType, ConfigurationStorage configurationStorage, DocumentsStorage documentsStorage, ServerStore serverStore)
        {
            var upgrade = new InternalUpgrader(storageType, configurationStorage, documentsStorage, serverStore);
            return upgrade.Upgrade;
        }
    }
}
