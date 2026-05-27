using System.Linq;
using Raven.Client.Documents.Operations.CdcSink.Test;

namespace Raven.AiAppliance.Wizard;

public sealed class TestMappingResponse
{
    public required TestMappingRowResponse[] Results { get; set; } = [];

    public required string[] Errors { get; set; } = [];

    public required string[] Warnings { get; set; } = [];

    internal static TestMappingResponse From(TestCdcSinkMappingResult result) => new()
    {
        Results = result.Results.Select(TestMappingRowResponse.From).ToArray(),
        Errors = result.Errors.ToArray(),
        Warnings = result.Warnings.ToArray(),
    };
}

public sealed class TestMappingRowResponse
{
    public string? DocumentId { get; set; }

    public string? Document { get; set; }

    public string? SourceRow { get; set; }

    public bool WouldDelete { get; set; }

    public bool IgnoreDeletes { get; set; }

    public string[] DebugOutput { get; set; } = [];

    public string? Error { get; set; }

    internal static TestMappingRowResponse From(TestCdcSinkRowResult row) => new()
    {
        DocumentId = row.DocumentId,
        Document = row.Document,
        SourceRow = row.SourceRow,
        WouldDelete = row.WouldDelete,
        IgnoreDeletes = row.IgnoreDeletes,
        DebugOutput = row.DebugOutput?.ToArray() ?? [],
        Error = row.Error,
    };
}
