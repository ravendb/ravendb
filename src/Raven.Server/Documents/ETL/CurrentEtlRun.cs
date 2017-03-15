namespace Raven.Server.Documents.ETL
{
    public class CurrentEtlRun
    {
        public int NumberOfExtractedItems;

        public long LastProcessedEtag;

        public void Reset()
        {
            NumberOfExtractedItems = 0;
            LastProcessedEtag = 0;
        }

        public void Errored()
        {
            LastProcessedEtag = -1;
        }
    }
}