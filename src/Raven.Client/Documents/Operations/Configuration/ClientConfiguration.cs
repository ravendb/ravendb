using Raven.Client.Http;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Configuration
{
    public class ClientConfiguration
    {
        public long Etag { get; set; }

        public bool Disabled { get; set; }

        public int? MaxNumberOfRequestsPerSession { get; set; }

        public bool? PrettifyGeneratedLinqExpressions { get; set; }

        public ReadBalanceBehavior? ReadBalanceBehavior { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(Etag)] = Etag,
                [nameof(MaxNumberOfRequestsPerSession)] = MaxNumberOfRequestsPerSession,
                [nameof(PrettifyGeneratedLinqExpressions)] = PrettifyGeneratedLinqExpressions,
                [nameof(ReadBalanceBehavior)] = ReadBalanceBehavior
            };
        }
    }
}
