namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlRunStats
    {
        public int NumberOfExtractedItems;

        public int NumberOfTransformedItems;

        public long LastTransformedEtag;

        public long LastLoadedEtag;

        public string BatchCompleteReason;
    }
}