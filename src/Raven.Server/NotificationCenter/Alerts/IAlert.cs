using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Alerts
{
    public interface IAlert
    {
        string Id { get; }

        DateTime? DismissedUntil { get; }

        DynamicJsonValue ToJson();
    }
}