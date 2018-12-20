using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Stats
{
    public class EtlTaskStats : IDynamicJson
    {
        public string TaskName { get; set; }

        public EtlProcessTransformationStats[] Stats { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TaskName)] = TaskName,
                [nameof(Stats)] = new DynamicJsonArray(Stats)
            };
        }
    }

    public class EtlProcessTransformationStats : IDynamicJson
    {
        public string TransformationName { get; set; }

        public EtlProcessStatistics Statistics { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TransformationName)] = TransformationName,
                [nameof(Statistics)] = Statistics.ToJson()
            };
        }
    }
}
