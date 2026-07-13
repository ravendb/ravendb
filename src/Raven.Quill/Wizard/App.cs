namespace Raven.Quill.Wizard;

/// <summary>
/// Per-app registry document stored in the appliance config DB. One App doc
/// per provisioned application. Created by W6 Provision; read by later
/// per-app routes (<c>/api/apps/{slug}/...</c>) to resolve slug → metadata.
///
/// Id is RavenDB-assigned via HiLo so the collection ends up at
/// <c>apps/1-A</c>, <c>apps/2-A</c>, etc. (See `[[project_in_tree_layout]]`
/// + design doc §3.1 for the `apps/{appId}` convention — the synthetic
/// "appId" field from the prototype is collapsed into Raven's natural
/// document id here.)
/// </summary>
internal sealed class App
{
    /// <summary>RavenDB-assigned (e.g. <c>apps/1-A</c>).</summary>
    public string? Id { get; set; }

    /// <summary>URL-safe handle derived from <see cref="AppName"/> via
    /// <see cref="Slugifier"/>. Unique across the appliance (enforced by
    /// the per-app database name, which equals the slug).</summary>
    public string Slug { get; set; } = "";

    /// <summary>Original caller input — preserved for UI display.</summary>
    public string AppName { get; set; } = "";

    /// <summary>Per-app RavenDB database name; always equals <see cref="Slug"/>.
    /// Duplicated as its own field so consumers don't have to remember the
    /// equality.</summary>
    public string Database { get; set; } = "";

    /// <summary>Name of the CDC Sink task on the per-app database.
    /// Always <c>{slug}-cdc</c>.</summary>
    public string CdcTaskName { get; set; } = "";

    /// <summary>UTC creation timestamp.</summary>
    public DateTime CreatedAt { get; set; }
}
