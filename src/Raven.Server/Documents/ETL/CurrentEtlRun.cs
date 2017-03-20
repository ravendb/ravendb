namespace Raven.Server.Documents.ETL
{
    public class CurrentEtlRun
    {
        public int NumberOfExtractedItems;

        public long LastTransformedEtag;

        public long LastLoadedEtag;

        public void Reset()
        {
            NumberOfExtractedItems = 0;
            LastTransformedEtag = 0;
            LastLoadedEtag = 0;
        }
    }
}