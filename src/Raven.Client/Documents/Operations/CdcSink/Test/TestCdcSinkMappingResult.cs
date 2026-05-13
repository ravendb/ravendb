using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink.Test;

/// <summary>
/// Response of <c>POST /admin/cdc-sink/test</c>. Always an array shape so single-row tests
/// and multi-row samples look the same to Studio. Per-row failures are isolated inside
/// each <see cref="TestCdcSinkRowResult.Error"/>; whole-request failures (validation,
/// connection, missing table) live in <see cref="Errors"/>.
/// </summary>
public class TestCdcSinkMappingResult : IDynamicJson
{
    public List<TestCdcSinkRowResult> Results { get; set; } = new();

    public List<string> Errors { get; set; } = new();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Results)] = new DynamicJsonArray(Results.Select(r => r.ToJson())),
            [nameof(Errors)] = new DynamicJsonArray(Errors),
        };
    }
}
