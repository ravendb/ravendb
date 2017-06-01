using Raven.Client.Documents.Replication.Messages;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlRunStats
    {
        public int NumberOfExtractedItems;

        public int NumberOfTransformedItems;

        public long LastTransformedEtag;

        public long LastLoadedEtag;

        public long LastFilteredOutEtag;

        public string BatchCompleteReason;

        public ChangeVectorEntry[] ChangeVector = new ChangeVectorEntry[0];
    }
}