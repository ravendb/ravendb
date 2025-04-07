using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial;

public class UpgradeRequired : IDynamicJson
{
    public bool AllowDismiss { get; set; }

    public DateTime AllowDismissUntil { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(AllowDismiss)] = AllowDismiss,
            [nameof(AllowDismissUntil)] = AllowDismissUntil
        };
    }
}
