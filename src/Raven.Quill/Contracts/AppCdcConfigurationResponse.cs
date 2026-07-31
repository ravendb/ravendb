using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.Contracts;

/// <summary>
/// An app's CDC sink configuration plus the source connection string it captures from, so the
/// wizard can reopen on the inputs the app runs on instead of asking for them again.
/// </summary>
public sealed record AppCdcConfigurationResponse(CdcSinkConfiguration Configuration, string? ConnectionString);
