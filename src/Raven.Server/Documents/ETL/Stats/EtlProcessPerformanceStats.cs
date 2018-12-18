using Raven.Client.Documents.Operations.ETL;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlTaskPerformanceStats
    {
        public long TaskId { get; set;  }
        
        public string TaskName { get; set; }

        public EtlType EtlType { get; set; }

        public EtlProcessPerformanceStats[] Stats { get; set; }
    }

    public class EtlProcessPerformanceStats
    {
        public string TransformationName { get; set; }
        public EtlPerformanceStats[] Performance { get; set; }
    }
}
