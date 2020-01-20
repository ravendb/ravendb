using System;
using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.MapReduce.Workers
{
    public class CleanupDocumentsForMapReduce : CleanupDocuments
    {
        private readonly MapReduceIndex _mapReduceIndex;

        public CleanupDocumentsForMapReduce(MapReduceIndex mapReduceIndex, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration, MapReduceIndexingContext mapReduceContext)
            : base(mapReduceIndex, documentsStorage, indexStorage, configuration, mapReduceContext)
        {
            _mapReduceIndex = mapReduceIndex;
        }

        public override bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            var moreWorkFound = base.Execute(databaseContext, indexContext, writeOperation, stats, token);

            if (_mapReduceIndex.OutputReduceToCollection?.HasDocumentsToDelete(indexContext) == true)
            {
                moreWorkFound = true;

                if (_mapReduceIndex.IsSideBySide() == false)
                {
                    // we can start deleting reduce output documents only if index becomes a regular one

                    moreWorkFound |= _mapReduceIndex.OutputReduceToCollection.DeleteDocuments(stats, indexContext);
                }
            }

            return moreWorkFound;
        }
    }
}
