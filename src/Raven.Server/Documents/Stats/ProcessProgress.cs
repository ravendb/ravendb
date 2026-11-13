using Voron.Data.Fixed;

namespace Raven.Server.Documents.Stats
{
    public abstract class ProcessProgress
    {
        public bool Completed { get; set; }

        public bool Estimated { get; set; }

        public double AverageProcessedPerSecond { get; set; }

        public long NumberOfDocumentsToProcess { get; set; }

        public long TotalNumberOfDocuments { get; set; }

        public long NumberOfDocumentTombstonesToProcess { get; set; }

        public long TotalNumberOfDocumentTombstones { get; set; }

        public long NumberOfCounterGroupsToProcess { get; set; }

        public long TotalNumberOfCounterGroups { get; set; }

        public long NumberOfTimeSeriesSegmentsToProcess { get; set; }

        public long TotalNumberOfTimeSeriesSegments { get; set; }

        public long NumberOfTimeSeriesDeletedRangesToProcess { get; set; }

        public long TotalNumberOfTimeSeriesDeletedRanges { get; set; }

        public void AddDocuments(in NumberOfEntriesAfterResult result)
        {
            NumberOfDocumentsToProcess += result.Count;
            TotalNumberOfDocuments += result.Total;
            Estimated |= result.Estimated;
        }

        public void AddDocumentTombstones(in NumberOfEntriesAfterResult result)
        {
            NumberOfDocumentTombstonesToProcess += result.Count;
            TotalNumberOfDocumentTombstones += result.Total;
            Estimated |= result.Estimated;
        }

        public void AddCounterGroups(in NumberOfEntriesAfterResult result)
        {
            NumberOfCounterGroupsToProcess += result.Count;
            TotalNumberOfCounterGroups += result.Total;
            Estimated |= result.Estimated;
        }

        public void AddTimeSeriesSegments(in NumberOfEntriesAfterResult result)
        {
            NumberOfTimeSeriesSegmentsToProcess += result.Count;
            TotalNumberOfTimeSeriesSegments += result.Total;
            Estimated |= result.Estimated;
        }

        public void AddTimeSeriesDeletedRanges(in NumberOfEntriesAfterResult result)
        {
            NumberOfTimeSeriesDeletedRangesToProcess += result.Count;
            TotalNumberOfTimeSeriesDeletedRanges += result.Total;
            Estimated |= result.Estimated;
        }
    }
}
