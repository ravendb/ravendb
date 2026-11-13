using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.Stats;

namespace Raven.Server.Documents.ETL.Stats
{
    public sealed class EtlTaskProgress
    {
        public string TaskName { get; set; }

        public EtlType EtlType { get; set; }

        public QueueBrokerType? QueueBrokerType { get; set; }

        public EtlProcessProgress[] ProcessesProgress { get; set; }
    }

    public sealed class EtlProcessProgress : ProcessProgress
    {
        public string TransformationName { get; set; }

        public string TransactionalId { get; set; }

        public bool Disabled { get; set; }
    }
}
