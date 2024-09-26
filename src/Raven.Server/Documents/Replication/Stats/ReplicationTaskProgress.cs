using Raven.Client.Documents.Replication;

namespace Raven.Server.Documents.Replication.Stats
{
    public sealed class ReplicationTaskProgress
    {
        public string TaskName { get; set; }

        public ReplicationNode.ReplicationType ReplicationType { get; set; }

        public ReplicationProcessProgress[] ProcessesProgress { get; set; }
    }

    public sealed class ReplicationProcessProgress
    {
        public string FromToString { get; set; }

        public bool Completed { get; set; }

        public long LastEtagSent { get; set; }

        public string DestinationChangeVector { get; set; }

        public string SourceChangeVector { get; set; }

        public long NumberOfDocumentsToProcess { get; set; }

        public long TotalNumberOfDocuments { get; set; }

        public long NumberOfDocumentTombstonesToProcess { get; set; }

        public long TotalNumberOfDocumentTombstones { get; set; }

        public long NumberOfRevisionsToProcess { get; set; }

        public long TotalNumberOfRevisions { get; set; }

        public long TotalNumberOfRevisionTombstones { get; set; }

        public long NumberOfAttachmentsToProcess { get; set; }

        public long TotalNumberOfAttachments { get; set; }

        public long TotalNumberOfAttachmentTombstones { get; set; }

        public long NumberOfCounterGroupsToProcess { get; set; }

        public long TotalNumberOfCounterGroups { get; set; }

        public long NumberOfTimeSeriesSegmentsToProcess { get; set; }

        public long TotalNumberOfTimeSeriesSegments { get; set; }

        public long NumberOfTimeSeriesDeletedRangesToProcess { get; set; }

        public long TotalNumberOfTimeSeriesDeletedRanges { get; set; }
    }
}
