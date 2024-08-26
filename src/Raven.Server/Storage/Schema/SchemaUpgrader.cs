using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Sparrow.Logging;
using Voron;
using Voron.Schema;

namespace Raven.Server.Storage.Schema
{
    public static class SchemaUpgrader
    {
        internal sealed class CurrentVersion
        {
            public const int ServerVersion = 62_000;

            public const int ConfigurationVersion = 50_000;

            public const int DocumentsVersion = 62_000;

            public const int LuceneIndexVersion = 62_000;

            public const int CoraxIndexVersion = 62_000;

            public static (int Version, StorageType Type) GetIndexVersionAndStorageType(SearchEngineType type) => type switch
            {
                SearchEngineType.Corax => (CoraxIndexVersion, StorageType.CoraxIndex),
                _ => (LuceneIndexVersion, StorageType.LuceneIndex)
            };
        }

        private static readonly int[] SkippedDocumentsVersion = { 40_012 };

        public enum StorageType
        {
            Server,
            Configuration,
            Documents,
            LuceneIndex,
            CoraxIndex
        }

        private sealed class InternalUpgrader
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
                bool shouldAddToInitLog = false;
                switch (_storageType)
                {
                    case StorageType.Server:
                        break;
                    case StorageType.Configuration:
                        break;
                    case StorageType.Documents:
                        shouldAddToInitLog = true;
                        if (SkippedDocumentsVersion.Contains(currentVersion))
                            throw new NotSupportedException(
                                $"Documents schema upgrade from version {currentVersion} is not supported, use the recovery tool to dump the data and then import it into a new database");
                        break;
                    case StorageType.CoraxIndex:
                    case StorageType.LuceneIndex:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(_storageType), _storageType, null);
                }

                versionAfterUpgrade = currentVersion;
                var updater = GetUpdater(_storageType, currentVersion);
                if (updater == null)
                    return false;

                versionAfterUpgrade = updater.To;

                if (shouldAddToInitLog)
                {
                    var msg = $"Started schema upgrade from version #{updater.From} to version #{updater.To}";
                    _documentsStorage.DocumentDatabase.AddToInitLog?.Invoke(LogLevel.Debug, msg);
                }

                bool result =  updater.Update(new UpdateStep(transactions)
                {
                    ConfigurationStorage = _configurationStorage,
                    DocumentsStorage = _documentsStorage,
                });

                if (shouldAddToInitLog)
                {
                    var msg = $"{(result ? "Finished" : "Failed to")} schema upgrade from version #{updater.From} to version #{updater.To}";
                    var logMode = result ? LogLevel.Debug : LogLevel.Info;
                    _documentsStorage.DocumentDatabase.AddToInitLog?.Invoke(logMode, msg);
                }

                return result;
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
                    case StorageType.LuceneIndex:
                        if (currentVersion >= 10 && currentVersion <= 12)
                            currentVersion += 40_000;
                        break;
                    case StorageType.CoraxIndex:
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
