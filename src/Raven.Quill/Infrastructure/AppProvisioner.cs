using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Quill.Wizard;

namespace Raven.Quill.Infrastructure;

internal static class AppProvisioner
{
    public static async Task<App> CreateAppAsync(
        IDocumentStore store, string slug, string appName, string cdcTaskName, CancellationToken ct)
    {
        await AppDatabaseFeatures.ConfigureAsync(store, slug, ct);

        var app = new App
        {
            Slug = slug,
            AppName = appName,
            Database = slug,
            CdcTaskName = cdcTaskName,
            CreatedAt = DateTime.UtcNow,
        };

        using var session = store.OpenAsyncSession();
        session.Advanced.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes;
        // slug-keyed id (not HiLo): avoids the W6->W7 index-staleness race (C1/C2)
        await session.StoreAsync(app, id: $"apps/{slug}", ct);
        await session.SaveChangesAsync(ct);
        return app;
    }
}
