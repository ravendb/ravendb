using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Indexes.Workers
{
    public class HandleSuggestions : IIndexingWork
    {
        private readonly Logger _logger;

        private readonly Index _index;
        private HashSet<string> _suggestionsFields;
        private readonly IndexingConfiguration _configuration;
        private readonly DocumentsStorage _documentsStorage;
        private readonly IndexStorage _indexStorage;

        public HandleSuggestions(Index index, HashSet<string> suggestionsFields, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration)
        {
            _index = index;
            _configuration = configuration;
            _suggestionsFields = suggestionsFields;
            _documentsStorage = documentsStorage;
            _indexStorage = indexStorage;
            _logger = LoggingSource.Instance
                .GetLogger<HandleReferences>(_indexStorage.DocumentDatabase.Name);
        }

        public string Name => "Suggestions";

        public bool CanContinueBatch(DocumentsOperationContext documentsContext, TransactionOperationContext indexingContext, IndexingStatsScope stats, long currentEtag, long maxEtag)
        {
            if (stats.Duration >= _configuration.MapTimeout.AsTimeSpan)
                return false;

            if (currentEtag >= maxEtag && stats.Duration >= _configuration.MapTimeoutAfterEtagReached.AsTimeSpan)
                return false;

            if (_index.CanContinueBatch(stats, documentsContext, indexingContext) == false)
                return false;

            return true;
        }

        public bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, CancellationToken token)
        {
            var pageSize = int.MaxValue;
            var maxTimeForDocumentTransactionToRemainOpen = Debugger.IsAttached == false
                ? _configuration.MaxTimeForDocumentTransactionToRemainOpen.AsTimeSpan
                : TimeSpan.FromMinutes(15);

            var moreWorkFound = DoHandleSuggestions(databaseContext, indexContext, writeOperation, stats, pageSize, maxTimeForDocumentTransactionToRemainOpen, token);

            return moreWorkFound;
        }

        private bool DoHandleSuggestions(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperation> writeOperation, IndexingStatsScope stats, int pageSize, TimeSpan maxTimeForDocumentTransactionToRemainOpen, CancellationToken token)
        {
            throw new NotImplementedException();
        }
    }
}
