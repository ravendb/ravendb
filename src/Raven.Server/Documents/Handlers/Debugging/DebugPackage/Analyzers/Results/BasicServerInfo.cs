using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

public class BasicServerInfo : IDynamicJson
{
    public string NodeTag { get; set; }
    public string Version { get; set; }
    public string ServerId { get; set; }
    public TimeSpan? UpTime { get; set; }
    public DateTime? StartUpTime { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(NodeTag)] = NodeTag,
            [nameof(Version)] = Version,
            [nameof(ServerId)] = ServerId,
            [nameof(UpTime)] = UpTime,
            [nameof(StartUpTime)] = StartUpTime
        };
    }
}
