using Raven.Client.Documents.Operations.ETL;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Stats
{
    public sealed class EtlTaskStats : IDynamicJson
    {
        public string TaskName { get; set; }

        public EtlType EtlType { get; set; }

        public string EtlSubType { get; set; }

        public long TaskId {get; set; }

        public string NodeTag { get; set; }

        public int? ShardNumber { get; set; }

        public EtlProcessTransformationStats[] Stats { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TaskName)] = TaskName,
                [nameof(TaskId)] = TaskId,
                [nameof(EtlType)] = EtlType.ToString(),
                [nameof(EtlSubType)] = EtlSubType,
                [nameof(NodeTag)] = NodeTag,
                [nameof(ShardNumber)] = ShardNumber,
                [nameof(Stats)] = new DynamicJsonArray(Stats)
            };
        }
    }

    public class EtlProcessTransformationStats : IDynamicJson
    {
        public string TransformationName { get; set; }

        public EtlProcessStatistics Statistics { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TransformationName)] = TransformationName,
                [nameof(Statistics)] = Statistics.ToJson()
            };
        }
    }
}
