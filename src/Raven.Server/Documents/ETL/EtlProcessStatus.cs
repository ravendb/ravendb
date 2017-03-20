namespace Raven.Server.Documents.ETL
{
    public class EtlProcessStatus
    {
        public string Name { get; set; } 

        public long LastProcessedEtag { get; set; }
    }
}