using System;
using System.Collections.Generic;

namespace Raven.Quill.Cdc;

/// <summary>
/// Mirrors the JSON RavenDB returns from <c>GET /databases/{db}/cdc-sink/errors</c>
/// (the shared task-errors shape: <c>{ "Results": [ { TaskName, ProcessErrors: [ err ],
/// ItemErrors: [ err ] } ] }</c>), parsed appliance-side with System.Text.Json so we don't
/// reference <c>Raven.Server</c>'s error types. Persistent per-task error store, distinct from
/// the rolling perf window — read only when the sink is reporting an error.
/// </summary>
internal sealed class CdcSinkErrorsRaw
{
    public List<CdcTaskErrorsRaw> Results { get; set; } = [];
}

internal sealed class CdcTaskErrorsRaw
{
    public string TaskName { get; set; } = "";
    public List<CdcTaskErrorRaw> ProcessErrors { get; set; } = [];
    public List<CdcTaskErrorRaw> ItemErrors { get; set; } = [];
}

/// <summary>One error row. <c>DocumentId</c> (item errors) and <c>AffectedDocumentsCount</c>
/// (process errors) are mutually exclusive — only the one for its list is set, the other is
/// null — so both are nullable, matching the <c>CdcError</c> contract.</summary>
internal sealed class CdcTaskErrorRaw
{
    public string TaskName { get; set; } = "";
    public DateTime CreatedAt { get; set; }
    public string Step { get; set; } = "";
    public string Error { get; set; } = "";
    public string? DocumentId { get; set; }
    public long? AffectedDocumentsCount { get; set; }
}
