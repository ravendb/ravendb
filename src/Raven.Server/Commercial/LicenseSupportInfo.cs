using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class LicenseSupportInfo
    {
        public Status Status { get; set; }

        public DateTimeOffset? EndsAt { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Status)] = Status,
                [nameof(EndsAt)] = EndsAt
            };
        }
    }

    public enum Status
    {
        NoSupport,

        PartialSupport,

        ProfessionalSupport,

        ProductionSupport,

        LicenseNotFound,

        InvalidStateSupportNotFound
    }
}
