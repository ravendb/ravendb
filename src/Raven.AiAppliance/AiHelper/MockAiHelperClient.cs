using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.AiAppliance.AiHelper;

/// <summary>
/// Demo-mode stand-in for <see cref="AiHelperInternalClient"/>. Returns canned Northwind sample
/// configs from <see cref="NorthwindSampleConfigs"/>, letting the front end exercise the wizard
/// and agent-Review UIs without a live api.ravendb.net connection.
/// Registered in <c>Program.cs</c> only in demo mode (the setup-package zip is mounted).
/// Production always uses the HTTP-backed client.
/// </summary>
public sealed class MockAiHelperClient : IAiHelperClient
{
    public Task<SuggestCdcInternalResult> SuggestCdcAsync(
        object? schema, object? samples, string prompt, CancellationToken ct)
    {
        var result = new SuggestCdcInternalResult(
            AiHelperStatus.Success,
            NorthwindSampleConfigs.BuildCdcConfig(),
            ["Mocked sample: mirrors the Northwind customers, orders, and products tables."],
            InputTokenCount: 128,
            OutputTokenCount: 256);

        return Task.FromResult(result);
    }

    public Task<SuggestAiAgentInternalResult> SuggestAiAgentAsync(
        CdcSinkConfiguration cdcConfig, object? collectionsSample, string mode, string? prompt, CancellationToken ct)
    {
        var fromPrompt = string.Equals(mode?.Trim(), "from-prompt", StringComparison.OrdinalIgnoreCase);

        var configurations = fromPrompt
            ? [NorthwindSampleConfigs.BuildPromptModeAgent(prompt)]
            : NorthwindSampleConfigs.BuildDataModeAgents();

        var rationale = fromPrompt
            ? new[] { "Mocked sample: a single agent derived from the supplied intent prompt." }
            : ["Mocked sample: three agents derived from the Northwind collections."];

        var result = new SuggestAiAgentInternalResult(
            AiHelperStatus.Success,
            configurations,
            rationale,
            InputTokenCount: 96,
            OutputTokenCount: 320);

        return Task.FromResult(result);
    }
}
