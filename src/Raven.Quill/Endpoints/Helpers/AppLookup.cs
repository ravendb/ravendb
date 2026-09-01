using Raven.Client.Documents;
using Raven.Quill.Wizard;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Quill.Contracts;

namespace Raven.Quill.Endpoints.Helpers;

internal static class AppLookup
{
    internal const string IdPrefix = "apps/";

    internal static string DocumentIdFor(string slug) => IdPrefix + slug;

    internal static async Task<App?> LoadAppAsync(IDocumentStore store, string slug, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession();
        // LoadAsync by slug-keyed id: no index, no staleness race
        return await session.LoadAsync<App>(DocumentIdFor(slug), ct);
    }

    internal static async Task DeleteAppAsync(IDocumentStore store, string slug, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession();
        session.Delete(DocumentIdFor(slug));
        session.Delete(WizardState.DocumentIdFor(slug));   // the app's in-progress wizard doc, if any
        await session.SaveChangesAsync(ct);
    }

    // The app database is the source of truth: the wizard's state document is scratch space
    // that a later setup run for the same slug may reset.
    internal static async Task<OngoingTaskCdcSink?> LoadCdcTaskAsync(IDocumentStore store, App app, CancellationToken ct)
    {
        var task = await store.Maintenance.ForDatabase(app.Database).SendAsync(
            new GetOngoingTaskInfoOperation(app.CdcTaskName, OngoingTaskType.CdcSink), ct);
        return task as OngoingTaskCdcSink;
    }

    internal static ApiErrorResponse NoCdcTaskError(string slug) => new($"no cdc task for '{slug}' found");

    internal static async Task<(CdcSinkTaskState? State, DateTime? LastModified)> LoadCdcStateAsync(IDocumentStore store, string database, string taskName, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);
        var state = await session.LoadAsync<CdcSinkTaskState>(CdcSinkTaskState.GetDocumentId(taskName), ct);
        if (state is null)
            return (null, null);

        return (state, session.Advanced.GetLastModifiedFor(state));
    }
}
