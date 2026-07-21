using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink.Test;

internal class CdcTestResult : IDynamicJson
{
    public bool Success { get; set; }
   
    public string Error { get; set; }

    public Dictionary<string, string> Sampled { get; set; } = new();

    public DynamicJsonValue ToJson() => new()
    {
        [nameof(Success)] = Success,
        [nameof(Error)] = Error,
        [nameof(Sampled)] = DynamicJsonValue.Convert(Sampled)
    };
}
