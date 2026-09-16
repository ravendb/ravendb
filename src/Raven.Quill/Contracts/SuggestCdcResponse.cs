using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.Contracts;

public sealed record SuggestCdcResponse(
    CdcSinkConfiguration? Configuration,
    IReadOnlyList<string> Rationale,
    string Status);
