using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Quill.Contracts;
using Raven.Quill.Hosting;
using Raven.Quill.Wizard;
using Raven.Server;

namespace QuillTests.E2E.Fixtures;

public sealed class QuillHost : IAsyncDisposable
{
    private const string SeededConnectionStringBaseName = "quill-seeded-llm";

    /// The app id the wizard wrappers send when a test doesn't care which app it is. Per-app-isolation tests
    /// pass explicit slugs instead. Matches the id the AI-helper base resets between tests.
    public const string DefaultWizardSlug = "wizard-test-app";

    private readonly ApplianceWebApplicationFactory _factory;
    private readonly ConcurrentDictionary<string, QuillApp> _apps = new();

    private QuillHost(ApplianceWebApplicationFactory factory, IDocumentStore config, RavenServer? server)
    {
        _factory = factory;
        Config = config;
        Server = server;
        Client = factory.CreateClient();
    }

    internal static async Task<QuillHost> CreateAsync(RavenServer? server, IDocumentStore config,
        string setupPackagePath = "",
        Action<ApplianceOptions>? configure = null, Action<IServiceCollection>? configureServices = null,
        bool seedChatConnectionString = true)
    {
        var factory = new ApplianceWebApplicationFactory(
            setupPackagePath: setupPackagePath,
            applianceStore: config,
            configureOptions: opts =>
            {
                opts.ConfigDatabase = config.Database;
                configure?.Invoke(opts);
            },
            configureServices: configureServices);

        var host = new QuillHost(factory, config, server);
        if (seedChatConnectionString)
            await host.PostConnectionStringAsync(new AiConnectionString
            {
                Name = SeededConnectionStringBaseName,
                ModelType = AiModelType.Chat,
                OllamaSettings = new OllamaSettings { Uri = "http://127.0.0.1:1/", Model = "llama3.1" },
            });

        return host;
    }

    public void AddApp(QuillApp app) => _apps.TryAdd(app.Slug, app);
    public void RemoveApp(QuillApp app) => _apps.TryRemove(app.Slug, out _);

    private async Task WaitForIndexingAsync()
    {
        foreach (var quillApp in _apps)
        {
            await quillApp.Value.WaitForIndexingAsync();
        }
    }

    public IDocumentStore Config { get; }

    /// The RavenDB server this host runs against, or null for the shared test server. Set only when the
    /// host needs its own server to isolate cluster-wide state (server-wide AI connection strings).
    public RavenServer? Server { get; }

    /// Carries the operator API key by default.
    public HttpClient Client { get; }

    public ApplianceWebApplicationFactory Factory => _factory;

    public IServiceProvider Services => _factory.Services;

    /// The prefixed name the seeded CS carries inside an app DB — what an agent's ConnectionStringName references.
    public string ConnectionStringName { get; } =
        ServerWideConnectionString.GetDatabaseRecordConnectionStringName(SeededConnectionStringBaseName);

    // ---- server-wide ----

    public Task<AiConnectionStringCreatedResponse> PostConnectionStringAsync(AiConnectionString body) =>
        QuillHttp.PostAsync<AiConnectionStringCreatedResponse>(Client, QuillRoutes.ConnectionStrings, body);

    public Task<IReadOnlyList<AiConnectionString>> GetConnectionStringsAsync() =>
        QuillHttp.GetAsync<IReadOnlyList<AiConnectionString>>(Client, QuillRoutes.ConnectionStrings);

    public Task<AiConnectionStringTestResponse> TestConnectionStringAsync(AiConnectionString body) =>
        QuillHttp.PostAsync<AiConnectionStringTestResponse>(Client, QuillRoutes.ConnectionStringsTest, body);

    public Task<AiConnectionString> GetConnectionStringAsync(string name) =>
        QuillHttp.GetAsync<AiConnectionString>(Client, QuillRoutes.ConnectionString(name));

    public Task DeleteConnectionStringAsync(string name) =>
        QuillHttp.DeleteAsync(Client, QuillRoutes.ConnectionString(name));

    /// A rejected connector / provider failure surfaces as a <see cref="QuillHttpException"/> (400 / 502).
    public Task<AiModelsResponse> PostAiModelsAsync(AiModelsRequest body) =>
        QuillHttp.PostAsync<AiModelsResponse>(Client, QuillRoutes.AiModels, body);

    public Task<AuthStatusResponse> GetAuthStatusAsync() =>
        QuillHttp.GetAsync<AuthStatusResponse>(Client, QuillRoutes.AuthStatus);

    public Task<LicenseResponse> GetLicenseAsync() =>
        QuillHttp.GetAsync<LicenseResponse>(Client, QuillRoutes.SettingsLicense);

    // /api/settings/usage is a license passthrough — no app index involved, so it doesn't wait on _apps.
    public Task<QuillUsageResponse> GetSettingsUsageAsync(int year, int? month = null, int? day = null) =>
        QuillHttp.GetAsync<QuillUsageResponse>(Client, $"{QuillRoutes.SettingsUsage}?{Periods.Query(year, month, day)}");

    public Task<ConnectResult> SetupConnectAsync(ConnectRequest body, string slug = DefaultWizardSlug) =>
        QuillHttp.PostAsync<ConnectResult>(Client, QuillRoutes.SetupConnect, body with { Slug = slug });

    public Task<DiscoverResponse> SetupDiscoverAsync(DiscoverRequest body, string slug = DefaultWizardSlug) =>
        QuillHttp.PostAsync<DiscoverResponse>(Client, QuillRoutes.SetupDiscover, body with { Slug = slug });

    public Task<CdcSinkConfiguration> SetupMapAsync(MapRequest body) =>
        QuillHttp.PostAsync<CdcSinkConfiguration>(Client, QuillRoutes.SetupMap, body);

    public Task<VerifyCdcResponse> VerifyCdcAsync(VerifyCdcRequest body, string slug = DefaultWizardSlug) =>
        QuillHttp.PostAsync<VerifyCdcResponse>(Client, QuillRoutes.SetupVerifyCdc, body with { Slug = slug });

    /// A non-success AI status still returns HTTP 200 with the status on the payload.
    public Task<SuggestCdcResponse> SuggestCdcAsync(SuggestCdcRequest body, string slug = DefaultWizardSlug) =>
        QuillHttp.PostAsync<SuggestCdcResponse>(Client, QuillRoutes.SuggestCdc, body with { Slug = slug });

    public Task<TestMappingResponse> TestMappingAsync(TestMappingRequest body, string slug = DefaultWizardSlug) =>
        QuillHttp.PostAsync<TestMappingResponse>(Client, QuillRoutes.SetupTestMapping, body with { Slug = slug });

    /// Creates the app the real way (DB named for the slug). The DB is not tracked as a <see cref="QuillApp"/>,
    /// so it is not deleted per-call; it is reclaimed when this host's server is disposed (own-server / collection host).
    public Task<ProvisionResponse> ProvisionAsync(ProvisionRequest body) =>
        QuillHttp.PostAsync<ProvisionResponse>(Client, QuillRoutes.SetupProvision, body);

    // ---- config-DB fan-out: waits on every app the test created before querying ----

    /// Summed across the config store's apps (or scoped to one via <paramref name="app"/>).
    public async Task<UsageResponse> GetUsageAsync(int year, int? month = null, int? day = null, string? app = null)
    {
        await WaitForIndexingAsync();
        var q = Periods.Query(year, month, day);
        if (app is not null) q += $"&app={app}";
        return await QuillHttp.GetAsync<UsageResponse>(Client, $"{QuillRoutes.Usage}?{q}");
    }

    public async Task<TokensByAppResponse> GetTokensByAppAsync()
    {
        await WaitForIndexingAsync();
        return await QuillHttp.GetAsync<TokensByAppResponse>(Client, QuillRoutes.UsageByApp);
    }

    public Task<IReadOnlyList<ApplianceAppResponse>> GetDashboardAppsAsync() =>
        QuillHttp.GetAsync<IReadOnlyList<ApplianceAppResponse>>(Client, QuillRoutes.DashboardApps);

    public Task<ApplianceAppResponse> GetDashboardAppAsync(string slug) =>
        QuillHttp.GetAsync<ApplianceAppResponse>(Client, QuillRoutes.DashboardApp(slug));

    // ---- app-scoped: slug as a parameter, so an unknown slug needs no QuillApp ----
    public Task DeleteAppAsync(string slug) =>
        QuillHttp.DeleteAsync(Client, QuillRoutes.App(slug));

    public Task<IReadOnlyList<AiConnectionString>> GetAppConnectionStringsAsync(string slug) =>
        QuillHttp.GetAsync<IReadOnlyList<AiConnectionString>>(Client, QuillRoutes.AppConnectionStrings(slug));

    public Task<ProvisionAgentResponse> ProvisionAgentAsync(string slug, AiAgentConfiguration body) =>
        ProvisionAgentAsync(slug, new EditAgentRequest(body, null));

    public Task<ProvisionAgentResponse> ProvisionAgentAsync(string slug, EditAgentRequest body) =>
        QuillHttp.PostAsync<ProvisionAgentResponse>(Client, QuillRoutes.SetupAgent(slug), body);

    public Task<IReadOnlyList<AgentSummaryResponse>> GetAgentsAsync(string slug) =>
        QuillHttp.GetAsync<IReadOnlyList<AgentSummaryResponse>>(Client, QuillRoutes.Agents(slug));

    public Task<AgentDetailsResponse> GetAgentAsync(string slug, string agentId) =>
        QuillHttp.GetAsync<AgentDetailsResponse>(Client, QuillRoutes.Agent(slug, agentId));

    public Task<ProvisionAgentResponse> EditAgentAsync(string slug, AiAgentConfiguration body) =>
        EditAgentAsync(slug, new EditAgentRequest(body, null));

    public Task<ProvisionAgentResponse> EditAgentAsync(string slug, EditAgentRequest body) =>
        QuillHttp.PostAsync<ProvisionAgentResponse>(Client, QuillRoutes.EditAgent(slug), body);

    public Task DeleteAgentAsync(string slug, string agentId) =>
        QuillHttp.DeleteAsync(Client, QuillRoutes.Agent(slug, agentId));

    /// A non-success AI status still returns HTTP 200 with the status on the payload.
    public Task<SuggestAgentResponse> SuggestAgentAsync(string slug, SuggestAgentRequest body) =>
        QuillHttp.PostAsync<SuggestAgentResponse>(Client, QuillRoutes.SuggestAgent(slug), body);

    public Task<ProvisionChannelResponse> ProvisionChannelAsync(string slug, ProvisionChannelRequest body) =>
        QuillHttp.PostAsync<ProvisionChannelResponse>(Client, QuillRoutes.SetupChannel(slug), body);

    public Task<IReadOnlyList<ChannelSummaryResponse>> GetChannelsAsync(string slug) =>
        QuillHttp.GetAsync<IReadOnlyList<ChannelSummaryResponse>>(Client, QuillRoutes.Channels(slug));

    public Task<ChannelSummaryResponse> UpdateChannelAsync(string slug, string channelId, UpdateChannelRequest body) =>
        QuillHttp.PutAsync<ChannelSummaryResponse>(Client, QuillRoutes.Channel(slug, channelId), body);

    public Task DeleteChannelAsync(string slug, string channelId) =>
        QuillHttp.DeleteAsync(Client, QuillRoutes.Channel(slug, channelId));

    public Task<IReadOnlyList<EmbedLinkSummaryResponse>> GetEmbedLinksAsync(string slug) =>
        QuillHttp.GetAsync<IReadOnlyList<EmbedLinkSummaryResponse>>(Client, QuillRoutes.EmbedLinks(slug));

    public Task<MintEmbedLinkResponse> MintEmbedLinkAsync(string slug, MintEmbedLinkRequest body) =>
        QuillHttp.PostAsync<MintEmbedLinkResponse>(Client, QuillRoutes.EmbedLinks(slug), body);

    public Task RevokeEmbedLinkAsync(string slug, string token) =>
        QuillHttp.DeleteAsync(Client, QuillRoutes.EmbedLink(slug, token));

    public Task<string> GetEmbedPageAsync(string slug, string token) =>
        QuillHttp.GetAsync<string>(Client, QuillRoutes.EmbedPage(slug, token));

    /// T=string returns the raw NDJSON body. Optional <paramref name="origin"/> exercises the per-link origin gate.
    public Task<string> SendEmbedChatAsync(string slug, string token, string prompt, string? origin = null, CancellationToken ct = default) =>
        QuillHttp.PostAsync<string>(Client, QuillRoutes.EmbedChat(slug, token), new EmbedChatRequest(prompt),
            configureRequest: origin is null ? null : req => req.Headers.TryAddWithoutValidation("Origin", origin),
            ct: ct);

    public Task<IReadOnlyList<ActivityEventDto>> GetActivityAsync(string slug) =>
        QuillHttp.GetAsync<IReadOnlyList<ActivityEventDto>>(Client, QuillRoutes.AppActivity(slug));

    public Task<AppOverviewResponse> GetOverviewAsync(string slug) =>
        QuillHttp.GetAsync<AppOverviewResponse>(Client, QuillRoutes.AppOverview(slug));

    public Task<AppUsageResponse> GetAppUsageAsync(string slug, int year, int? month = null, int? day = null) =>
        QuillHttp.GetAsync<AppUsageResponse>(Client, $"{QuillRoutes.AppUsage(slug)}?{Periods.Query(year, month, day)}");

    public Task<IReadOnlyList<DataCollectionDto>> GetCollectionsAsync(string slug) =>
        QuillHttp.GetAsync<IReadOnlyList<DataCollectionDto>>(Client, QuillRoutes.AppCollections(slug));

    public Task<ChannelStatsResponse> GetChannelStatsAsync(string slug) =>
        QuillHttp.GetAsync<ChannelStatsResponse>(Client, QuillRoutes.AppChannelStats(slug));

    public Task<ConversationStatsResponse> GetConversationStatsAsync(string slug, int year, int? month = null, int? day = null) =>
        QuillHttp.GetAsync<ConversationStatsResponse>(Client, $"{QuillRoutes.AppConversationStats(slug)}?{Periods.Query(year, month, day)}");

    public Task<ConversationListResult> GetConversationsAsync(string slug, int year, int? start = null, int? pageSize = null)
    {
        var q = Periods.Query(year, month: null, day: null);
        if (start is not null) q += $"&start={start}";
        if (pageSize is not null) q += $"&pageSize={pageSize}";
        return QuillHttp.GetAsync<ConversationListResult>(Client, $"{QuillRoutes.AppConversations(slug)}?{q}");
    }

    /// Caller passes the raw conversation document id, e.g. a percent-encoded <c>chats%2Frecent</c>.
    public Task<ConversationDto> GetConversationAsync(string slug, string conversationId) =>
        QuillHttp.GetAsync<ConversationDto>(Client, $"{QuillRoutes.AppConversations(slug)}/{conversationId}");

    public Task<AppCdcConfigurationResponse> GetCdcAsync(string slug) =>
        QuillHttp.GetAsync<AppCdcConfigurationResponse>(Client, QuillRoutes.AppCdc(slug));

    public Task<CdcPerformanceResponse> GetCdcPerformanceAsync(string slug) =>
        QuillHttp.GetAsync<CdcPerformanceResponse>(Client, QuillRoutes.AppCdcPerformance(slug));

    public Task<IReadOnlyList<CdcError>> GetCdcErrorsAsync(string slug) =>
        QuillHttp.GetAsync<IReadOnlyList<CdcError>>(Client, QuillRoutes.AppCdcErrors(slug));

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await _factory.DisposeAsync();
        // non-null only for an own-server host; disposing it reclaims every app database created on it
        Server?.Dispose();
    }
}
