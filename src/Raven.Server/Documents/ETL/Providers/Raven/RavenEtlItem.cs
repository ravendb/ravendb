namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenEtlItem : ExtractedItem
    {
        public RavenEtlItem(Document document) : base(document)
        {
        }

        public RavenEtlItem(DocumentTombstone tombstone) : base(tombstone)
        {
        }
        
    }
}