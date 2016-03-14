namespace Raven.Server.Documents.Indexes
{
    public class IndexingBatchStats
    {
        public int IndexingAttempts;
        public int IndexingSuccesses;
        public int IndexingErrors;

        public override string ToString()
        {
            return $"Attempts: {IndexingAttempts}. Successes: {IndexingSuccesses}. Errors: {IndexingErrors}";
        }
    }
}
