using System.Collections.Generic;

using Raven.Abstractions;
using Raven.Client.Data;
using Raven.Server.Exceptions;

namespace Raven.Server.Documents.Indexes
{
    public class IndexingRunStats
    {
        public int IndexingAttempts;
        public int IndexingSuccesses;
        public int IndexingErrors;
        public int IndexingOutputs;

        public List<IndexingError> Errors;

        public override string ToString()
        {
            return $"Attempts: {IndexingAttempts}. Successes: {IndexingSuccesses}. Errors: {IndexingErrors}";
        }

        public void AddMapError(string key, string message)
        {
            AddError(key, message, "Map");
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
                Action = action ?? string.Empty,
                Document = key ?? string.Empty,
                Timestamp = SystemTime.UtcNow,
                Error = message ?? string.Empty
            });
        }
    }
}
