using Raven.Client.Documents.Operations.ETL;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlTaskProgress
    {
        public string TaskName { get; set; }

        public EtlType EtlType { get; set; }

        public EtlProcessProgress[] ProcessesProgress { get; set; }
    }

    public class EtlProcessProgress
    {
        public string TransformationName { get; set; }

        public bool Completed => (NumberOfDocumentsToProcess > 0 || NumberOfDocumentTombstonesToProcess > 0 ||
                                 NumberOfCountersToProcess > 0 || NumberOfCounterTombstonesToProcess > 0) == false;

        public bool Disabled { get; set; }

        public double AverageProcessedPerSecond { get; set; }

        public long LastProcessedEtag { get; set; }

        public long NumberOfDocumentsToProcess { get; set; }

        public long TotalNumberOfDocuments { get; set; }

        public long NumberOfDocumentTombstonesToProcess { get; set; }

        public long TotalNumberOfDocumentTombstones { get; set; }

        public long NumberOfCountersToProcess { get; set; }

        public long TotalNumberOfCounters { get; set; }

        public long NumberOfCounterTombstonesToProcess { get; set; }

        public long TotalNumberOfCounterTombstones { get; set; }
    }
}
