using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public sealed class LicenseSupportInfo
    {
        public Status Status { get; set; }

        public DateTimeOffset? EndsAt { get; set; }

        public LicenseSupportType SupportType { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Status)] = Status,
                [nameof(EndsAt)] = EndsAt,
                [nameof(SupportType)] = SupportType
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

    public enum LicenseSupportType 
    {
        None,
        Regular,
        Extended
    }
}
