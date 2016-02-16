using System;

using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoIndex : MapIndex<AutoIndexDefinition>
    {
        private AutoIndex(int indexId, AutoIndexDefinition definition)
            : base(indexId, IndexType.Auto, definition)
        {
        }

        public static AutoIndex CreateNew(int indexId, AutoIndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = new AutoIndex(indexId, definition);
            instance.Initialize(documentDatabase);

            return instance;
        }

        public static AutoIndex Open(int indexId, StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var instance = new AutoIndex(indexId, null);
            instance.Initialize(environment, documentDatabase);

            throw new NotImplementedException();

            return instance;
        }
    }
}