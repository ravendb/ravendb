using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.Contracts;

public sealed record AppCdcConfigurationResponse(CdcSinkConfiguration Configuration, string? ConnectionString);
