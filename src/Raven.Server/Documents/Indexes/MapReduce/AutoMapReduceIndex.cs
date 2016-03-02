using System;
using Raven.Server.Documents.Indexes.Auto;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class AutoMapReduceIndex : Index<AutoIndexDefinition>
    {
        private AutoMapReduceIndex(int indexId, AutoIndexDefinition definition)
            : base(indexId, IndexType.Auto, definition)
        {
        }

        public static AutoMapReduceIndex CreateNew(int indexId, AutoIndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var instance = new AutoMapReduceIndex(indexId, definition);
            instance.Initialize(documentDatabase);

            return instance;
        }

        public static AutoMapReduceIndex Open(int indexId, StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            throw new NotImplementedException();
            //var definition = AutoIndexDefinition.Load(environment);
            //var instance = new AutoMapReduceIndex(indexId, definition);
            //instance.Initialize(environment, documentDatabase);

            //return instance;
        }
    }
}