using System.Runtime.CompilerServices;
using Raven.Server.Documents.Indexes;
using Voron.Debugging;

namespace Raven.Server.Indexing
{
    public class IndexOutputFilesSummary
    {
        public long TotalWritten { get; private set; }

        public bool HasVoronWriteErrors { get; private set; }

        public IndexingStatsScope CommitStats { get; private set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Increment(long value)
        {
            TotalWritten += value;
        }

        public void Reset()
        {
            TotalWritten = 0;
            HasVoronWriteErrors = false;
        }

        public void SetCommitStats(IndexingStatsScope commitStats)
        {
            CommitStats = commitStats;
        }

        public void SetWriteError()
        {
            HasVoronWriteErrors = true;
        }
    }
}
