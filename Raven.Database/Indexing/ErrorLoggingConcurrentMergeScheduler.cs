using System;
using Lucene.Net.Index;
using Raven.Abstractions.Logging;

namespace Raven.Database.Indexing
{
    public class ErrorLoggingConcurrentMergeScheduler : ConcurrentMergeScheduler
    {
        public ErrorLoggingConcurrentMergeScheduler(string indexName)
        {
            IndexName = indexName;
        }
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        public string IndexName { get;}

        protected override void HandleMergeException(System.Exception exc)
        {
            try
            {
                base.HandleMergeException(exc);
            }
            catch (Exception e)
            {
                log.WarnException($"Concurrent merge failed for index: {IndexName}", e);
            }
        }
    }
}
