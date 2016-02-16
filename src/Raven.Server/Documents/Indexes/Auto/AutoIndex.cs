using System;
using System.Diagnostics;
using System.Linq;

using Raven.Server.Config.Categories;

using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoIndex : MapIndex<AutoIndexDefinition>
    {
        private AutoIndex(int indexId, AutoIndexDefinition definition)
            : base(indexId, IndexType.Auto, definition)
        {
        }

        public static AutoIndex CreateNew(int indexId, AutoIndexDefinition definition, DocumentsStorage documentsStorage, IndexingConfiguration indexingConfiguration, DatabaseNotifications databaseNotifications)
        {
            var instance = new AutoIndex(indexId, definition);
            instance.Initialize(documentsStorage, indexingConfiguration, databaseNotifications);

            return instance;
        }

        public static AutoIndex Open(int indexId, StorageEnvironment environment, DocumentsStorage documentsStorage, IndexingConfiguration indexingConfiguration, DatabaseNotifications databaseNotifications)
        {
            var instance = new AutoIndex(indexId, null);
            instance.Initialize(environment, documentsStorage, indexingConfiguration, databaseNotifications);

            throw new NotImplementedException();

            return instance;
        }
    }
}