using Raven.Client.Documents.Operations.CdcSink.Test;

namespace Raven.Quill.Wizard;

public sealed class VerifyCdcResponse
{
    public required bool Success { get; set; }

    public required WizardError[] Errors { get; set; }

    public required string[] Warnings { get; set; }

    public required string[] CompletedTables { get; set; }

    internal static VerifyCdcResponse From(CdcTestResult result) => new()
    {
        Success = result.Success,
        Errors = result.Success ? [] : [WizardErrorFormatter.Format(result.Error)],
        Warnings = result.Warnings.ToArray(),
        CompletedTables = result.CompletedTables.ToArray(),
    };

    internal static VerifyCdcResponse Failed(params string[] errors) => new()
    {
        Success = false,
        Errors = errors.Select(WizardErrorFormatter.Format).ToArray(),
        Warnings = [],
        CompletedTables = [],
    };
}
