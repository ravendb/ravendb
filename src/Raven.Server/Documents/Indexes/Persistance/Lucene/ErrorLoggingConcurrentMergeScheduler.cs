using System;

using Lucene.Net.Index;

using Raven.Abstractions.Logging;

namespace Raven.Server.Documents.Indexes.Persistance.Lucene
{
    public class ErrorLoggingConcurrentMergeScheduler : ConcurrentMergeScheduler
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(ErrorLoggingConcurrentMergeScheduler));

        protected override void HandleMergeException(Exception exc)
        {
            try
            {
                base.HandleMergeException(exc);
            }
            catch (Exception e)
            {
                Log.WarnException("Concurrent merge failed", e);
            }
        }
    }
}
