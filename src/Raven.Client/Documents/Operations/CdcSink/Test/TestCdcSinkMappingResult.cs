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
internal class TestCdcSinkMappingResult : IDynamicJson
{
    public List<TestCdcSinkRowResult> Results { get; set; } = new();

    /// <summary>
    /// Hard failures that prevent the test from producing results — validation, connection
    /// resolution, source-table row fetch, etc. A populated <see cref="Errors"/> usually means
    /// <see cref="Results"/> is empty.
    /// </summary>
    public List<string> Errors { get; set; } = new();

    /// <summary>
    /// Advisory notes that don't prevent the test from running — for example, "linked /
    /// embedded tables on this table are not exercised in test mode (root mapping only)".
    /// Studio can surface them in a different colour from <see cref="Errors"/> so the user
    /// knows the results in <see cref="Results"/> are still valid.
    /// </summary>
    public List<string> Warnings { get; set; } = new();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Results)] = new DynamicJsonArray(Results.Select(r => r.ToJson())),
            [nameof(Errors)] = new DynamicJsonArray(Errors),
            [nameof(Warnings)] = new DynamicJsonArray(Warnings),
        };
    }
}
