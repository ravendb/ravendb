using System;
using System.Linq;
using Raven.Server.Documents;
using Voron;
using Voron.Impl;

namespace Raven.Server.Storage.Schema
{
    public static class SchemaUpgrader
    {
        internal class CurrentVersion
        {
            public const int ServerVersion = 12;

            public const int ConfigurationVersion = 11;

            public const int DocumentsVersion = 16;

            public const int IndexVersion = 12;
        }

        private static readonly int[] SkippedDocumentsVersion = { 12 };
        
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

            internal InternalUpgrader(StorageType storageType, ConfigurationStorage configurationStorage, DocumentsStorage documentsStorage)
            {
                _storageType = storageType;
                _configurationStorage = configurationStorage;
                _documentsStorage = documentsStorage;
            }

            internal bool Upgrade(Transaction readTx, Transaction writeTx, int currentVersion, out int versionAfterUpgrade)
            {
                switch (_storageType)
                {
                    case StorageType.Server:
                        break;
                    case StorageType.Configuration:
                        break;
                    case StorageType.Documents:
                        if (SkippedDocumentsVersion.Contains(currentVersion))
                        {
                            throw new NotSupportedException($"Documents schema upgrade from version {currentVersion} is not supported, use the recovery tool to dump the data and then import it into a new database");
                        }
                        break;
                    case StorageType.Index:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(_storageType), _storageType, null);
                }
                
                versionAfterUpgrade = currentVersion;
                var name = $"Raven.Server.Storage.Schema.Updates.{_storageType.ToString()}.From{currentVersion}";
                var schemaUpdateType = typeof(SchemaUpgrader).Assembly.GetType(name);
                if (schemaUpdateType == null)
                    return false;

                versionAfterUpgrade++;

                switch (_storageType)
                {
                    case StorageType.Server:
                        break;
                    case StorageType.Configuration:
                        break;
                    case StorageType.Documents:
                        while (SkippedDocumentsVersion.Contains(versionAfterUpgrade))
                        {
                            versionAfterUpgrade++;
                        }
                        break;
                    case StorageType.Index:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(_storageType), _storageType, null);
                }
                
                var schemaUpdate = (ISchemaUpdate)Activator.CreateInstance(schemaUpdateType);
                return schemaUpdate.Update(new UpdateStep
                {
                    ReadTx = readTx,
                    WriteTx = writeTx,
                    ConfigurationStorage = _configurationStorage,
                    DocumentsStorage = _documentsStorage,
                });
            }
        }
        
        public static UpgraderDelegate Upgrader(StorageType storageType, ConfigurationStorage configurationStorage, DocumentsStorage documentsStorage)
        {
            var upgrade = new InternalUpgrader(storageType, configurationStorage, documentsStorage);
            return upgrade.Upgrade;
        }
    }
}
