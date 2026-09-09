using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink.Test;

internal class CdcTestResult : IDynamicJson
{
    public bool Success { get; set; }
    public string Error { get; set; }
    public List<string> CompletedTables { get; set; } = new();
    public List<string> Warnings { get; set; } = new();

    public DynamicJsonValue ToJson() => new()
    {
        [nameof(Success)] = Success,
        [nameof(Error)] = Error,
        [nameof(CompletedTables)] = CompletedTables,
        [nameof(Warnings)] = new DynamicJsonArray(Warnings)
    };
}
