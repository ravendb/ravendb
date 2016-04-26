using System.Threading;

namespace Raven.Server.Documents.Indexes
{
    public class IndexIdentities
    {
        private int indexingStatsId;

        public int GetNextIndexingStatsId()
        {
            return Interlocked.Increment(ref indexingStatsId);
        }
    }
}