using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;

namespace Raven.Quill.Wizard;

/// <summary>
/// Single-tenant wizard state, persisted at the fixed id <see cref="DocumentId"/>
/// in <c>quill-config</c>. Each wizard step overwrites its slice; no
/// sessionId, no TTL, no GC (per Ayende RavenDB-26629).
/// </summary>
internal sealed class WizardState
{
    public const string DocumentId = "wizard-state";

    public string? Provider { get; set; }

    // ConnectionString is intentionally NOT persisted here. Credentials live
    // only on the registered SqlConnectionString named "_wizard-source-probe"
    // in the config DB — one source of truth. Provision (next slice) reads
    // that registered connection string when transplanting it into the
    // per-app DB.

    public ConnectResult? LastVerifyResult { get; set; }
    public DateTime? LastVerifyAt { get; set; }

    // CdcSinkSourceSchema is internal in Raven.Client — accessible here via
    // InternalsVisibleTo("Raven.Quill"). Persisting it couples the
    // wizard-state doc shape to the internal schema-discovery shape; accepted
    // trade-off for in-tree code (forces the enclosing type to be internal too).
    public CdcSinkSourceSchema? LastDiscoveredSchema { get; set; }
    public DateTime? LastDiscoverAt { get; set; }

    // The map config the admin built / pasted in W3. W4 Test-mapping reads
    // this back when proxying to /admin/cdc-sink/test; W6 Provision reads it
    // when registering the actual CDC Sink task on the per-app DB.
    // CdcSinkConfiguration is public in Raven.Client, so no friend-visibility
    // gymnastics needed here.
    public CdcSinkConfiguration? LastMapConfiguration { get; set; }
    public DateTime? LastMapAt { get; set; }
}
