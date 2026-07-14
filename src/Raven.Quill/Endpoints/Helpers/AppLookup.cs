using Raven.Client.Documents;
using Raven.Quill.Wizard;
using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.Endpoints.Helpers;

/// <summary>
/// The one place that resolves an app slug to its <see cref="App"/> doc.
/// LoadAsync (not Query) — the App doc id is slug-keyed (<c>apps/{slug}</c>),
/// so no index and no staleness race against an immediately-prior provision in
/// the wizard chain. Callers own their unknown-slug 404 response shape.
/// </summary>
internal static class AppLookup
{
    internal static async Task<App?> LoadAppAsync(IDocumentStore store, string slug, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession();
        return await session.LoadAsync<App>($"apps/{slug}", ct);
    }

    internal static async Task<(CdcSinkTaskState State, DateTime LastModified)> LoadCdcStateAsync(IDocumentStore store, string database, string taskName, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);
        var state = await session.LoadAsync<CdcSinkTaskState>(CdcSinkTaskState.GetDocumentId(taskName), ct);
        var lastModified = session.Advanced.GetLastModifiedFor(state);
        return (state, lastModified ?? default);
    }
}
