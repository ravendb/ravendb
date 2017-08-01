using Sparrow.Json.Parsing;

namespace Raven.Client.Server
{
    public class ClientConfiguration
    {
        public bool Disabled { get; set; }

        public int? MaxNumberOfRequestsPerSession { get; set; }

        public bool? PrettifyGeneratedLinqExpressions { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(MaxNumberOfRequestsPerSession)] = MaxNumberOfRequestsPerSession,
                [nameof(PrettifyGeneratedLinqExpressions)] = PrettifyGeneratedLinqExpressions
            };
        }
    }
}