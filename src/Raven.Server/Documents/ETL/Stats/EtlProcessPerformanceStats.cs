namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlProcessPerformanceStats
    {
        public string ProcessName { get; set; }
        public EtlPerformanceStats[] Performance { get; set; }
    }
}