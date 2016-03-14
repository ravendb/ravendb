using System;
using System.Collections.Generic;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Server.Exceptions;

namespace Raven.Server.Documents.Indexes
{
    public class IndexingBatchStats
    {
        private readonly int _indexId;

        private readonly string _indexName;

        public int IndexingAttempts;
        public int IndexingSuccesses;
        public int IndexingErrors;

        public List<IndexingError> Errors;

        public IndexingBatchStats(int indexId, string indexName)
        {
            _indexId = indexId;
            _indexName = indexName;
        }

        public override string ToString()
        {
            return $"Attempts: {IndexingAttempts}. Successes: {IndexingSuccesses}. Errors: {IndexingErrors}";
        }

        public void AddMapError(string key, string message)
        {
            AddError(key, message, "Map");
        }

        public void AddStatsError(Exception exception)
        {
            AddError(null, $"Could not update statistics: {exception.Message}", "Stats");
        }

        public void AddWriteError(IndexWriteException exception)
        {
            AddError(null, $"Write exception occured: {exception.Message}", "Write");
        }

        public void AddAnalyzerError(IndexAnalyzerException exception)
        {
            AddError(null, $"Could not create analyzer: {exception.Message}", "Analyzer");
        }

        private void AddError(string key, string message, string action)
        {
            if (Errors == null)
                Errors = new List<IndexingError>();

            Errors.Add(new IndexingError
            {
                Id = -1, // TODO [ppekrol]
                Action = action,
                Index = _indexId,
                IndexName = _indexName,
                Document = key,
                Timestamp = SystemTime.UtcNow,
                Error = message
            });
        }
    }
}
