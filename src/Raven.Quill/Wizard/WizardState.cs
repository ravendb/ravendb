using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;

namespace Raven.Quill.Wizard;

internal sealed class WizardState
{
    public static string DocumentIdFor(string slug) => $"wizard-state/{slug}";

    public string? Provider { get; set; }

    public string? SourceConnectionString { get; set; }

    public ConnectResult? LastVerifyResult { get; set; }
    public DateTime? LastVerifyAt { get; set; }

    public CdcSinkSourceSchema? LastDiscoveredSchema { get; set; }
    public DateTime? LastDiscoverAt { get; set; }

    public CdcSinkConfiguration? LastMapConfiguration { get; set; }
    public DateTime? LastMapAt { get; set; }
}
