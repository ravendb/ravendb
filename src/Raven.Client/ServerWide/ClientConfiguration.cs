using Raven.Client.Http;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide
{
    public class ClientConfiguration
    {
        public long Etag { get; set; }

        public bool Disabled { get; set; }

        public int? MaxNumberOfRequestsPerSession { get; set; }

        public bool? PretifyGeneratedLinqExpressions { get; set; }

        public ReadBalanceBehavior? ReadBalanceBehavior { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(Etag)] = Etag,
                [nameof(MaxNumberOfRequestsPerSession)] = MaxNumberOfRequestsPerSession,
                [nameof(PretifyGeneratedLinqExpressions)] = PretifyGeneratedLinqExpressions,
                [nameof(ReadBalanceBehavior)] = ReadBalanceBehavior

            };
        }
    }
}
