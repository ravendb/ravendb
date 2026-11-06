using Raven.Client.Documents.Indexes;
using Voron.Data.Fixed;

namespace Raven.Server.Documents.Indexes
{
    internal static class IndexProgressExtensions
    {
        public static void AddItems(this IndexProgress.CollectionStats stats, in NumberOfEntriesAfterResult result)
        {
            stats.NumberOfItemsToProcess += result.Count;
            stats.TotalNumberOfItems += result.Total;
            stats.Estimated |= result.Estimated;
        }

        public static void AddTombstones(this IndexProgress.CollectionStats stats, in NumberOfEntriesAfterResult result)
        {
            stats.NumberOfTombstonesToProcess += result.Count;
            stats.TotalNumberOfTombstones += result.Total;
            stats.Estimated |= result.Estimated;
        }

        public static void AddTimeSeriesDeletedRanges(this IndexProgress.CollectionStats stats, in NumberOfEntriesAfterResult result)
        {
            stats.NumberOfTimeSeriesDeletedRangesToProcess += result.Count;
            stats.TotalNumberOfTimeSeriesDeletedRanges += result.Total;
            stats.Estimated |= result.Estimated;
        }
    }
}
