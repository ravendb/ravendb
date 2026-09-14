using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Quill.Feedback;
using Raven.Quill.Hosting;
using Raven.Quill.Wizard;
using Xunit;

namespace QuillTests.E2E.Fixtures;

// Per-collection fixtures and bases for the test groups that share one host instead of booting a RavenServer per
// test. Each is the same shape: a QuillCollectionHost carrying the group's test double, and a base that attaches
// the double via an overridden NewHostAsync and resets it per test. Collection definitions live in QuillCollections.

// ---- AI-helper (Suggest Agent / Cdc) ----

/// Collection host for the AI-helper endpoint tests: the shared host plus one <see cref="MockQuillServices"/> the host
/// is pointed at, both reused across the collection instead of built per test.
public sealed class QuillAiHelperFixture : QuillCollectionHost
{
    public MockQuillServices Mock { get; private set; } = null!;

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        Mock = await MockQuillServices.StartAsync();
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await Mock.DisposeAsync();
    }
}

/// Base for the two AI-helper classes. Points the collection host at the shared mock (with a generous assist
/// timeout, so any test not exercising a timeout or an unreachable service uses it), and resets the mock and the
/// config-DB wizard singleton before each test.
public abstract class QuillAiHelperTestBase(ITestOutputHelper output, QuillAiHelperFixture fixture)
    : QuillTestBase(output, fixture)
{
    protected MockQuillServices Mock => fixture.Mock;

    protected override Task<QuillHost> NewHostAsync(
        Action<ApplianceOptions>? configure = null, Action<IServiceCollection>? configureServices = null,
        string setupPackagePath = "", bool seedChatConnectionString = true, bool longLived = false) =>
        base.NewHostAsync(
            configure: opts =>
            {
                opts.AiApiUrl = fixture.Mock.BaseAddress;
                opts.AiAssistTimeout = TimeSpan.FromSeconds(30);
                configure?.Invoke(opts);   // a test can override the URL/timeout for its own host
            },
            configureServices: configureServices,
            setupPackagePath: setupPackagePath.Length == 0 ? NewDataPath(forceCreateDir: true) : setupPackagePath,
            seedChatConnectionString: seedChatConnectionString,
            longLived: longLived);

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();   // Host is the shared AI host, built via the override above
        Mock.Reset();

        // the wizard doc is keyed per app; the suggest wrappers use DefaultWizardSlug, so clear that one so a
        // test starts clean (no-op for the agent class, which never writes one)
        using var session = Host.Config.OpenAsyncSession();
        session.Delete(WizardState.DocumentIdFor(QuillHost.DefaultWizardSlug));
        await session.SaveChangesAsync();
    }
}

// ---- feedback ----

/// A resettable <see cref="IFeedbackSender"/> that records the last request; reused across a collection so the
/// feedback tests share one host instead of building one per test.
internal sealed class RecordingFeedbackSender : IFeedbackSender
{
    public SendFeedbackRequest? Request { get; private set; }
    public string? UserAgent { get; private set; }

    /// What <see cref="SendAsync"/> returns; a test flips it to exercise the send-failed path.
    public bool SendResult { get; set; } = true;

    public Task<bool> SendAsync(SendFeedbackRequest request, string userAgent, CancellationToken token)
    {
        Request = request;
        UserAgent = userAgent;
        return Task.FromResult(SendResult);
    }

    public void Reset()
    {
        Request = null;
        UserAgent = null;
        SendResult = true;
    }
}

public sealed class QuillFeedbackFixture : QuillCollectionHost
{
    internal RecordingFeedbackSender Feedback { get; } = new();
}

/// Base for the feedback tests: swaps the recording sender into the shared collection host and resets it per test.
public abstract class QuillFeedbackTestBase(ITestOutputHelper output, QuillFeedbackFixture fixture)
    : QuillTestBase(output, fixture)
{
    internal RecordingFeedbackSender Feedback => fixture.Feedback;

    protected override Task<QuillHost> NewHostAsync(
        Action<ApplianceOptions>? configure = null, Action<IServiceCollection>? configureServices = null,
        string setupPackagePath = "", bool seedChatConnectionString = true, bool longLived = false) =>
        base.NewHostAsync(configure,
            services =>
            {
                services.RemoveAll<IFeedbackSender>();
                services.AddSingleton<IFeedbackSender>(fixture.Feedback);
                configureServices?.Invoke(services);
            },
            setupPackagePath, seedChatConnectionString, longLived);

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        Feedback.Reset();
    }
}

// ---- Telegram ----

/// Collection host for the Telegram channel tests: the shared host plus one <see cref="MockTelegramBotApi"/> the
/// host's bot-client factory is pointed at, and one <see cref="FakeAgentRouter"/> so pipeline tests assert on
/// dispatched requests without a live LLM.
public sealed class QuillTelegramFixture : QuillCollectionHost
{
    public MockTelegramBotApi Mock { get; private set; } = null!;

    public MockOpenAiApi Llm { get; private set; } = null!;

    internal FakeAgentRouter Router { get; } = new();

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        Mock = await MockTelegramBotApi.StartAsync();
        Llm = await MockOpenAiApi.StartAsync();
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await Mock.DisposeAsync();
        await Llm.DisposeAsync();
    }
}

/// Base for the Telegram tests: points the shared collection host at the mock Bot API, shrinks the streaming
/// edit debounce, swaps in the recording router, and resets both per test. Pollers stay off the process-wide
/// SharedAppliance because every Telegram channel is provisioned on this collection's own host.
public abstract class QuillTelegramTestBase(ITestOutputHelper output, QuillTelegramFixture fixture)
    : QuillTestBase(output, fixture)
{
    protected MockTelegramBotApi Mock => fixture.Mock;

    internal FakeAgentRouter Router => fixture.Router;

    protected override Task<QuillHost> NewHostAsync(
        Action<ApplianceOptions>? configure = null, Action<IServiceCollection>? configureServices = null,
        string setupPackagePath = "", bool seedChatConnectionString = true, bool longLived = false) =>
        base.NewHostAsync(
            configure: opts =>
            {
                ApplyTelegramOptions(opts);
                configure?.Invoke(opts);
            },
            configureServices: services =>
            {
                services.RemoveAll<Raven.Quill.Agents.IAgentRouter>();
                services.AddSingleton<Raven.Quill.Agents.IAgentRouter>(fixture.Router);
                configureServices?.Invoke(services);
            },
            setupPackagePath: setupPackagePath, seedChatConnectionString: seedChatConnectionString,
            longLived: longLived);

    protected MockOpenAiApi Llm => fixture.Llm;

    protected Task<QuillHost> NewRealRouterHostAsync() =>
        base.NewHostAsync(configure: ApplyTelegramOptions, seedChatConnectionString: false);

    private void ApplyTelegramOptions(ApplianceOptions opts)
    {
        opts.Telegram.ApiUrl = fixture.Mock.BaseAddress;
        opts.Telegram.EditDebounce = TimeSpan.FromMilliseconds(50);
        opts.Telegram.ApplyChangesInterval = TimeSpan.FromMilliseconds(250);
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        Mock.Reset();
        Router.Reset();
        Llm.Reset();
    }

    protected async Task<int> WaitForPollingToSettleAsync(string token, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(20));
        var last = Mock.GetUpdatesCallCount(token);
        var quietWindows = 0;

        while (true)
        {
            await Task.Delay(400);

            var current = Mock.GetUpdatesCallCount(token);
            if (current == last)
            {
                // one quiet window can be a scheduler stall on a loaded runner; require two in a row
                if (++quietWindows >= 2)
                    return current;
                continue;
            }

            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Telegram polling never settled; still at {current} calls");

            quietWindows = 0;
            last = current;
        }
    }

    /// Unique per test so captures/counters on the shared mock never bleed across tests in the collection.
    protected static string NewBotToken() =>
        $"{Random.Shared.NextInt64(1_000_000, 9_999_999)}:AA{Guid.NewGuid():N}";
}

// ---- Slack ----

public sealed class QuillSlackFixture : QuillCollectionHost
{
    public MockSlackApi Slack { get; private set; } = null!;

    internal FakeAgentRouter Router { get; } = new();

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        Slack = await MockSlackApi.StartAsync();
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await Slack.DisposeAsync();
    }
}

public abstract class QuillSlackTestBase(ITestOutputHelper output, QuillSlackFixture fixture)
    : QuillTestBase(output, fixture)
{
    protected MockSlackApi Slack => fixture.Slack;

    internal FakeAgentRouter Router => fixture.Router;

    protected override Task<QuillHost> NewHostAsync(
        Action<ApplianceOptions>? configure = null, Action<IServiceCollection>? configureServices = null,
        string setupPackagePath = "", bool seedChatConnectionString = true, bool longLived = false) =>
        base.NewHostAsync(
            configure: opts =>
            {
                opts.Slack.ApiUrl = fixture.Slack.BaseAddress;
                opts.Slack.EditDebounce = TimeSpan.FromMilliseconds(50);
                configure?.Invoke(opts);
            },
            configureServices: services =>
            {
                services.RemoveAll<Raven.Quill.Agents.IAgentRouter>();
                services.AddSingleton<Raven.Quill.Agents.IAgentRouter>(fixture.Router);
                configureServices?.Invoke(services);
            },
            setupPackagePath: setupPackagePath, seedChatConnectionString: seedChatConnectionString,
            longLived: longLived);

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        Slack.Reset();
        Router.Reset();
    }

    protected static string NewBotToken() => "xoxb-" + Guid.NewGuid().ToString("N");

    protected static string NewTeamId() => "T" + Random.Shared.Next(100_000_000, 999_999_999);

    protected static string NewBotUserId() => "U" + Random.Shared.Next(100_000_000, 999_999_999);
}

// ---- Discord ----

public sealed class QuillDiscordFixture : QuillCollectionHost
{
    public MockDiscordApi Discord { get; private set; } = null!;

    internal FakeAgentRouter Router { get; } = new();

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        Discord = await MockDiscordApi.StartAsync();
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await Discord.DisposeAsync();
    }
}

public abstract class QuillDiscordTestBase(ITestOutputHelper output, QuillDiscordFixture fixture)
    : QuillTestBase(output, fixture)
{
    protected MockDiscordApi Discord => fixture.Discord;

    protected static readonly TimeSpan GatewayRestartDelay = TimeSpan.FromSeconds(3);

    internal FakeAgentRouter Router => fixture.Router;

    protected override Task<QuillHost> NewHostAsync(
        Action<ApplianceOptions>? configure = null, Action<IServiceCollection>? configureServices = null,
        string setupPackagePath = "", bool seedChatConnectionString = true, bool longLived = false) =>
        base.NewHostAsync(
            configure: opts =>
            {
                opts.Discord.ApiUrl = fixture.Discord.BaseAddress;
                opts.Discord.EditDebounce = TimeSpan.FromMilliseconds(50);
                opts.Discord.ApplyChangesInterval = TimeSpan.FromSeconds(1);
                opts.Discord.GatewayBackoffMax = TimeSpan.FromMilliseconds(200);
                opts.Discord.GatewayHandshakeTimeout = TimeSpan.FromSeconds(2);
                opts.Discord.GatewayRestartDelay = GatewayRestartDelay;
                configure?.Invoke(opts);
            },
            configureServices: services =>
            {
                services.RemoveAll<Raven.Quill.Agents.IAgentRouter>();
                services.AddSingleton<Raven.Quill.Agents.IAgentRouter>(fixture.Router);
                configureServices?.Invoke(services);
            },
            setupPackagePath: setupPackagePath, seedChatConnectionString: seedChatConnectionString,
            longLived: longLived);

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        Discord.Reset();
        Router.Reset();
    }

    protected static string NewBotToken() =>
        "MT" + Guid.NewGuid().ToString("N")[..8] + ".G" + Guid.NewGuid().ToString("N")[..6] + "." +
        Guid.NewGuid().ToString("N");

    protected static string NewApplicationId() =>
        Random.Shared.NextInt64(100_000_000_000_000_000, 999_999_999_999_999_999).ToString();

    protected static string NewBotUserId() =>
        Random.Shared.NextInt64(100_000_000_000_000_000, 999_999_999_999_999_999).ToString();
}

// ---- AI models ----

/// A resettable <see cref="IAiHelperClient"/> that records the last call and returns a per-test-configurable
/// (transport, content) pair; reused across a collection so the models tests share one host.
internal sealed class RecordingAiHelperClient : IAiHelperClient
{
    public string? Path { get; private set; }
    public string? Method { get; private set; }
    public object? Request { get; private set; }

    public AiHelperStatus Transport { get; set; } = AiHelperStatus.Success;
    public string Content { get; set; } = string.Empty;

    public Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, string method, object request, CancellationToken ct)
    {
        Path = path;
        Method = method;
        Request = request;
        return Task.FromResult((Transport, Content));
    }

    public Task<SuggestCdcInternalResult> SuggestCdcAsync(object? schema, object? samples, string prompt, CancellationToken ct) =>
        throw new NotSupportedException();

    public Task<SuggestAiAgentInternalResult> SuggestAiAgentAsync(CdcSinkConfiguration cdcConfig, object? collectionsSample, string mode, string? prompt, CancellationToken ct) =>
        throw new NotSupportedException();

    public Task<HttpResponseMessage> SendChatAsync(string message, string? conversationId, CancellationToken ct) =>
        throw new NotSupportedException();

    public Task<AiHelperStatus> CheckConsentAsync(CancellationToken ct) =>
        throw new NotSupportedException();

    public Task<AiHelperStatus> GiveConsentAsync(CancellationToken ct) =>
        throw new NotSupportedException();

    public Task<T> DeserializeAsync<T>(string json, CancellationToken ct) where T : class =>
        throw new NotSupportedException();

    public void Reset()
    {
        Path = null;
        Method = null;
        Request = null;
        Transport = AiHelperStatus.Success;
        Content = string.Empty;
    }
}

public sealed class QuillAiModelsFixture : QuillCollectionHost
{
    internal RecordingAiHelperClient AiHelper { get; } = new();
}

/// Base for the models tests: swaps the recording AI-helper client into the shared collection host and resets it per test.
public abstract class QuillAiModelsTestBase(ITestOutputHelper output, QuillAiModelsFixture fixture)
    : QuillTestBase(output, fixture)
{
    internal RecordingAiHelperClient AiHelper => fixture.AiHelper;

    protected override Task<QuillHost> NewHostAsync(
        Action<ApplianceOptions>? configure = null, Action<IServiceCollection>? configureServices = null,
        string setupPackagePath = "", bool seedChatConnectionString = true, bool longLived = false) =>
        base.NewHostAsync(configure,
            services =>
            {
                services.RemoveAll<IAiHelperClient>();
                services.AddSingleton<IAiHelperClient>(fixture.AiHelper);
                configureServices?.Invoke(services);
            },
            setupPackagePath, seedChatConnectionString, longLived);

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        AiHelper.Reset();
    }
}
