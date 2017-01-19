using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Alerts
{
    public interface IAlert
    {
        string AlertId { get; }

        DateTime? DismissedUntil { get; }

        DynamicJsonValue ToJson();
    }
}