using System;
using Raven.Client.Http;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Configuration
{
    public class ClientConfiguration
    {
        public long Etag { get; set; }

        public bool Disabled { get; set; }

        public int? MaxNumberOfRequestsPerSession { get; set; }

        [Obsolete("This feature is currently not implemented and does not have any effect on the generated LINQ expressions")]
        public bool? PrettifyGeneratedLinqExpressions { get; set; }

        public ReadBalanceBehavior? ReadBalanceBehavior { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Disabled)] = Disabled,
                [nameof(Etag)] = Etag,
                [nameof(MaxNumberOfRequestsPerSession)] = MaxNumberOfRequestsPerSession,
#pragma warning disable CS0618 // Type or member is obsolete
                [nameof(PrettifyGeneratedLinqExpressions)] = PrettifyGeneratedLinqExpressions,
#pragma warning restore CS0618 // Type or member is obsolete
                [nameof(ReadBalanceBehavior)] = ReadBalanceBehavior
            };
        }
    }
}
