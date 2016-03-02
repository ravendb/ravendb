using System;

using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoMapIndex : Index<AutoIndexDefinition>
    {
        private AutoMapIndex(int indexId, AutoIndexDefinition definition)
            : base(indexId, IndexType.Auto, definition)
        {
        }

        public static AutoMapIndex CreateNew(int indexId, AutoIndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = new AutoMapIndex(indexId, definition);
            instance.Initialize(documentDatabase);

            return instance;
        }

        public static AutoMapIndex Open(int indexId, StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var definition = AutoIndexDefinition.Load(environment);
            var instance = new AutoMapIndex(indexId, definition);
            instance.Initialize(environment, documentDatabase);

            return instance;
        }
    }
}