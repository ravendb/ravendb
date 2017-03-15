namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public class SqlEtlStatus
    {
        public string Name { get; set; } 

        public long LastProcessedEtag { get; set; }
    }
}