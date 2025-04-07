using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial;

public class LicenseVersionInformation : IDynamicJson
{
    public int Major { get; set; }
    public int Minor { get; set; }
    public DateTime UpdatedAt { get; set; }

    public Version Version => new Version(Major, Minor);

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Major)] = Major,
            [nameof(Minor)] = Minor,
            [nameof(UpdatedAt)] = UpdatedAt
        };
    }
}
