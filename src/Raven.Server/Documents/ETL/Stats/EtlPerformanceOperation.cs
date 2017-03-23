namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlPerformanceOperation
    {
        public string Name { get; set; }

        public EtlPerformanceOperation[] Operations { get; set; }
    }
}