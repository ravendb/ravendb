using System.Collections.Generic;

namespace Raven.Client.Documents.Indexes
{
    internal sealed class IndexProgress
    {
        public string Name { get; set; }

        public IndexType Type { get; set; }

        public IndexSourceType SourceType { get; set; }

        public Dictionary<string, CollectionStats> Collections { get; set; }

        public bool IsStale { get; set; }

        public IndexRunningStatus IndexRunningStatus { get; set; }

        public double ProcessedPerSecond { get; set; }

        public RollingIndex IndexRollingStatus { get; set; }

        public sealed class CollectionStats
        {
            public long LastProcessedItemEtag { get; set; }

            public long NumberOfItemsToProcess { get; set; }

            public long TotalNumberOfItems { get; set; }

            public long LastProcessedTombstoneEtag { get; set; }

            public long LastProcessedTimeSeriesDeletedRangeEtag { get; set; }

            public long NumberOfTombstonesToProcess { get; set; }

            public long NumberOfTimeSeriesDeletedRangesToProcess { get; set; }

            public long TotalNumberOfTombstones { get; set; }

            public long TotalNumberOfTimeSeriesDeletedRanges { get; set; }

            internal void UpdateLastEtag(long lastEtag, bool isTombstone)
            {
                if (isTombstone)
                {
                    LastProcessedTombstoneEtag = lastEtag;
                }
                else
                {
                    LastProcessedItemEtag = lastEtag;
                }
            }

            internal void UpdateTimeSeriesDeletedRangeLastEtag(long lastEtag)
            {
                LastProcessedTimeSeriesDeletedRangeEtag = lastEtag;
            }
        }
    }

    internal sealed class IndexesProgress
    {
        public IndexProgress[] Results { get; set; }
    }
}
