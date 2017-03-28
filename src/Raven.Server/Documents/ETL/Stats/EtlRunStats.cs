namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlRunStats
    {
        public int NumberOfExtractedItems;

        public long LastTransformedEtag;

        public long LastLoadedEtag;

        public string BatchCompleteReason;
    }
}