using Sparrow.Json.Parsing;

namespace Raven.Client.Server.ETL
{
    public class EtlProcessStatus
    {
        public string Destination { get; set; }

        public string TransformationName { get; set; }

        public long LastProcessedEtag { get; set; }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(Destination)] = Destination,
                [nameof(TransformationName)] = TransformationName,
                [nameof(LastProcessedEtag)] = LastProcessedEtag
            };

            return json;
        }

        public static string GenerateItemName(string destinationName, string transformationName)
        {
            return $"etl/{destinationName}/{transformationName}"; // TODO arek - also need a db name to ensure uniqueness ??
        }
    }
}