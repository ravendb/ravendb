using FastTests;
using Microsoft.Extensions.DependencyInjection;
using Raven.Quill.Hosting;
using Raven.Quill.Infrastructure;
using Xunit;

[assembly: AssemblyFixture(typeof(QuillTests.E2E.Fixtures.SharedApplianceReaper))]

namespace QuillTests.E2E.Fixtures;

public class QuillCollectionHost : IAsyncLifetime
{
    private QuillHost? _host;

    internal async Task<QuillHost> GetAsync(Func<Task<QuillHost>> factory) => _host ??= await factory();

    public virtual ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public virtual async ValueTask DisposeAsync()
    {
        if (_host is not null)
            await _host.DisposeAsync();   // once, at collection end
    }
}

public abstract class QuillTestBase : RavenTestBase
{
    private readonly QuillCollectionHost? _collection;

    protected QuillTestBase(ITestOutputHelper output, QuillCollectionHost? collection = null) : base(output)
    {
        _collection = collection;
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        // A collection host is built once by the first test in the collection and reused across it (tests in a
        // collection run serially), then disposed by the fixture — its server, and every app database on it, with it.
        if (_collection is not null)
        {
            Host = await _collection.GetAsync(() => NewHostAsync(longLived: true));
            return;
        }

        Host = await SharedAppliance.GetAsync(() => BuildHostCoreAsync(longLived: true));
    }

    /// The shared host — the appliance under test. Most tests use this.
    protected QuillHost Host { get; set; } = null!;

    protected virtual Task<QuillHost> NewHostAsync(
        Action<ApplianceOptions>? configure = null, Action<IServiceCollection>? configureServices = null,
        string setupPackagePath = "", bool seedChatConnectionString = true, bool longLived = false) =>
        BuildHostCoreAsync(configure, configureServices, setupPackagePath, seedChatConnectionString, longLived);

    private async Task<QuillHost> BuildHostCoreAsync(
        Action<ApplianceOptions>? configure = null, Action<IServiceCollection>? configureServices = null,
        string setupPackagePath = "", bool seedChatConnectionString = true, bool longLived = false)
    {
        var server = GetNewServer(new ServerCreationOptions { RegisterForDisposal = false });
        var config = GetDocumentStore(new Options
        {
            Server = server,
            ModifyDatabaseName = _ => "quill-config",
        });

        if (longLived)
            CreatedStores.TryRemove(config);

        return await QuillHost.CreateAsync(server, config,
            setupPackagePath: setupPackagePath, configure: configure, configureServices: configureServices,
            seedChatConnectionString: seedChatConnectionString);
    }

    private protected async Task<QuillApp> NewAppAsync(QuillHost? host = null)
    {
        host ??= Host;

        var slug = "app-" + Guid.NewGuid().ToString("N");   // well-formed slug == database name
        var store = GetDocumentStore(new Options
        {
            ModifyDatabaseName = _ => slug,
            Server = host.Server,
            // the delete endpoint hard-deletes the database, so teardown must not race it
            DeleteDatabaseOnDispose = false,
        });

        await AppProvisioner.CreateAppAsync(host.Config, slug, appName: slug, cdcTaskName: $"{slug}-cdc", CancellationToken.None);

        var app = new QuillApp(this, host, store, slug);
        return app;
    }
}

internal static class SharedAppliance
{
    private static QuillHost? _host;
    private static readonly SemaphoreSlim Gate = new(1, 1);

    public static async Task<QuillHost> GetAsync(Func<Task<QuillHost>> build)
    {
        if (_host is not null)
            return _host;

        await Gate.WaitAsync();
        try
        {
            return _host ??= await build();
        }
        finally
        {
            Gate.Release();
        }
    }

    public static async ValueTask DisposeAsync()
    {
        var host = Interlocked.Exchange(ref _host, null);
        if (host is not null)
            await host.DisposeAsync();
    }
}

public sealed class SharedApplianceReaper : IAsyncLifetime
{
    public ValueTask InitializeAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync() => SharedAppliance.DisposeAsync();
}
