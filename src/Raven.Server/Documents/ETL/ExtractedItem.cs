namespace Raven.Server.Documents.ETL
{
    public abstract class ExtractedItem
    {
        public long Etag { get; protected set; }

        public bool IsDelete { get; protected set; }
    }
}