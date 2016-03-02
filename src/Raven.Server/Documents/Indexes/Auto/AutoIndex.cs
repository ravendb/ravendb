using System;

using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoIndex : Index<AutoIndexDefinition>
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
            var definition = AutoIndexDefinition.Load(environment);
            var instance = new AutoIndex(indexId, definition);
            instance.Initialize(environment, documentDatabase);

            return instance;
        }
    }
}