using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.AiAppliance.Contracts;

/// <summary>
/// AI-suggested CDC mapping draft. <see cref="Configuration"/> is null when
/// <see cref="Status"/> is not <c>Success</c> (e.g. OutOfTokens, InvalidCredentials).
/// Generate-only: the draft populates the editable Review card; persistence stays with
/// <c>POST /api/setup/map</c>.
/// </summary>
public sealed record SuggestCdcResponse(
    CdcSinkConfiguration? Configuration,
    IReadOnlyList<string> Rationale,
    string Status);
