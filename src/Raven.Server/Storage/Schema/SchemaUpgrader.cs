using System;
using Raven.Server.Documents;
using Voron.Impl;

namespace Raven.Server.Storage.Schema
{
    public static class SchemaUpgrader
    {
        internal class CurrentVersion
        {
            public const int ServerVersion = 10;

            public const int ConfigurationVersion = 10;

            public const int DocumentsVersion = 11;

            public const int IndexVersion = 10;
        }

        public enum StorageType
        {
            Server,
            Configuration,
            Documents,
            Index,
        }

        public static Func<Transaction, Transaction, int, bool> Upgrader(StorageType storageType, 
            ConfigurationStorage configurationStorage, DocumentsStorage documentsStorage)
        {
            return (readTx, writeTx, currentVersion) =>
            {
                var name = $"Raven.Server.Storage.Schema.Updates.{storageType.ToString()}.From{currentVersion}";
                var schemaUpdateType = typeof(SchemaUpgrader).Assembly.GetType(name);
                if (schemaUpdateType == null)
                    return false;

                var schemaUpdate = (ISchemaUpdate)Activator.CreateInstance(schemaUpdateType);
                return schemaUpdate.Update(new UpdateStep
                {
                    ReadTx = readTx,
                    WriteTx = writeTx,
                    ConfigurationStorage = configurationStorage,
                    DocumentsStorage = documentsStorage,
                });
            };
        }
    }
}
