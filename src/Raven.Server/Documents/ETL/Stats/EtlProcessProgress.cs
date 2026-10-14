using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Voron.Data.Fixed;

namespace Raven.Server.Documents.ETL.Stats
{
    public sealed class EtlTaskProgress
    {
        public string TaskName { get; set; }

        public EtlType EtlType { get; set; }
        
        public QueueBrokerType? QueueBrokerType { get; set; }

        public EtlProcessProgress[] ProcessesProgress { get; set; }
    }

    public sealed class EtlProcessProgress
    {
        public string TransformationName { get; set; }
        
        public string TransactionalId { get; set; }

        public bool Completed { get; set; }

        public bool Disabled { get; set; }

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
