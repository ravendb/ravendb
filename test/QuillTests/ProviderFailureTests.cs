using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// Every way an AI provider can misbehave used to reach the visitor as one sentence pointing at a log file
/// they cannot read. These pin what each class of failure says now, and which ones are worth retrying.
public class ProviderFailureTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string RateLimitBody = """{"error":{"message":"Rate limit reached","type":"rate_limit_error"}}""";
    private const string BadKeyBody = """{"error":{"message":"Incorrect API key provided","type":"invalid_request_error"}}""";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_rate_limit_short_enough_to_wait_out_is_retried_and_the_turn_answers()
    {
        await using var mock = await MockQuillServices.StartAsync(new FinalTurn("""{"reply":"answered after the wait"}"""));
        mock.CompletionFailure = (429, RateLimitBody);
        mock.CompletionFailureHeaders["Retry-After"] = "1";
        mock.CompletionFailureCount = 1;

        await using var h = await HarnessAsync(mock);

        var ndjson = await h.App.SendEmbedChatAsync(h.Token, "hello");

        Assert.Equal("answered after the wait", ReplyOf(ndjson));
        Assert.Equal(2, mock.CompletionAttempts);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_rate_limit_asking_for_longer_than_we_hold_the_request_fails_without_retrying()
    {
        await using var mock = await MockQuillServices.StartAsync();
        mock.CompletionFailure = (429, RateLimitBody);
        mock.CompletionFailureHeaders["Retry-After"] = "42";

        await using var h = await HarnessAsync(mock);

        var error = ErrorOf(await h.App.SendEmbedChatAsync(h.Token, "hello"));

        Assert.Equal(1, mock.CompletionAttempts);
        Assert.Equal("provider_busy", error.Code);
        Assert.True(error.Retryable);
        Assert.Contains("busy", error.Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_rate_limit_that_keeps_coming_back_is_retried_to_the_cap_and_then_reported()
    {
        await using var mock = await MockQuillServices.StartAsync();
        mock.CompletionFailure = (429, RateLimitBody);
        mock.CompletionFailureHeaders["Retry-After"] = "1";

        await using var h = await HarnessAsync(mock);

        var error = ErrorOf(await h.App.SendEmbedChatAsync(h.Token, "hello"));

        // the first attempt plus the two retries the cap allows
        Assert.Equal(3, mock.CompletionAttempts);
        Assert.True(error.Retryable);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_rejected_key_is_a_failure_the_visitor_cannot_retry_and_never_names_the_setting()
    {
        await using var mock = await MockQuillServices.StartAsync();
        mock.CompletionFailure = (401, BadKeyBody);

        await using var h = await HarnessAsync(mock);

        var error = ErrorOf(await h.App.SendEmbedChatAsync(h.Token, "hello"));

        Assert.Equal(1, mock.CompletionAttempts);
        Assert.Equal("chat_failed", error.Code);
        Assert.False(error.Retryable);
        Assert.DoesNotContain("key", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("server logs", error.Message);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_provider_outage_is_reported_as_worth_retrying()
    {
        await using var mock = await MockQuillServices.StartAsync();
        mock.CompletionFailure = (500, """{"error":{"message":"boom"}}""");

        await using var h = await HarnessAsync(mock);

        var error = ErrorOf(await h.App.SendEmbedChatAsync(h.Token, "hello"));

        Assert.Equal("provider_busy", error.Code);
        Assert.True(error.Retryable);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_completion_with_no_content_fails_instead_of_recording_an_empty_answer()
    {
        await using var mock = await MockQuillServices.StartAsync(new EmptyTurn());
        await using var h = await HarnessAsync(mock);

        var ndjson = await h.App.SendEmbedChatAsync(h.Token, "hello");

        Assert.DoesNotContain("\"type\":\"done\"", ndjson);
        Assert.False(ErrorOf(ndjson).Retryable);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_failed_turn_gives_the_visitor_their_invocation_back()
    {
        await using var mock = await MockQuillServices.StartAsync();
        mock.CompletionFailure = (401, BadKeyBody);

        await using var h = await HarnessAsync(mock);
        await h.App.SendEmbedChatAsync(h.Token, "hello");

        using var session = h.App.Store.OpenAsyncSession(h.App.Slug);
        var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + h.Token);
        Assert.Equal(0, link.InvocationCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_provider_that_never_answers_is_cut_off_at_the_turn_deadline()
    {
        await using var mock = await MockQuillServices.StartAsync(new FinalTurn("""{"reply":"too late"}"""));
        mock.CompletionDelay = TimeSpan.FromSeconds(60);

        var host = await NewHostAsync(configure: options => options.AgentTurnDeadline = TimeSpan.FromSeconds(2));
        await using var h = await HarnessAsync(mock, host);

        var started = DateTime.UtcNow;
        var error = ErrorOf(await h.App.SendEmbedChatAsync(h.Token, "hello"));
        var elapsed = DateTime.UtcNow - started;

        Assert.True(elapsed < TimeSpan.FromSeconds(30),
            $"the turn should be abandoned at the deadline rather than waiting the provider out, took {elapsed}");
        Assert.True(error.Retryable);
        Assert.Contains("too long", error.Message);
    }

    /// The operator is behind the API key, so this surface names what to go and fix.
    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_operator_facing_draft_run_names_the_provider_reason()
    {
        await using var mock = await MockQuillServices.StartAsync();
        mock.CompletionFailure = (401, BadKeyBody);

        await using var h = await HarnessAsync(mock);

        using var response = await Host.Client.PostAsJsonAsync(QuillRoutes.SetupTry(h.App.Slug), new
        {
            prompt = "hello",
            configuration = new
            {
                name = "Draft Agent",
                systemPrompt = "You are a draft agent.",
                connectionStringName = h.ConnectionStringName,
            },
        });

        var error = ErrorOf(await response.Content.ReadAsStringAsync());

        Assert.Contains("rejected the credentials", error.Message);
        Assert.Contains("Incorrect API key provided", error.Message);
        Assert.DoesNotContain("server logs", error.Message);
    }

    private sealed record ChatError(string Message, string Code, bool Retryable);

    private static ChatError ErrorOf(string ndjson)
    {
        foreach (var line in Lines(ndjson))
        {
            using var parsed = JsonDocument.Parse(line);
            if (parsed.RootElement.GetProperty("type").GetString() != "error")
                continue;

            return new ChatError(
                parsed.RootElement.GetProperty("message").GetString() ?? "",
                parsed.RootElement.GetProperty("code").GetString() ?? "",
                parsed.RootElement.GetProperty("retryable").GetBoolean());
        }

        throw new InvalidOperationException($"no error frame in: {ndjson}");
    }

    private static string? ReplyOf(string ndjson)
    {
        foreach (var line in Lines(ndjson))
        {
            using var parsed = JsonDocument.Parse(line);
            if (parsed.RootElement.GetProperty("type").GetString() == "done")
                return parsed.RootElement.GetProperty("answer").GetProperty("reply").GetString();
        }

        throw new InvalidOperationException($"no done frame in: {ndjson}");
    }

    private static IEnumerable<string> Lines(string ndjson) =>
        ndjson.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => line.Trim())
            .Where(line => line.Length > 0);

    private sealed record Harness(QuillApp App, string Token, string ConnectionStringName) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => App.DisposeAsync();
    }

    private async Task<Harness> HarnessAsync(MockQuillServices mock, QuillHost? host = null)
    {
        host ??= Host;

        var app = await NewAppAsync(host);

        var connectionStringName = "mock-llm-" + Guid.NewGuid().ToString("N");
        await host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = connectionStringName,
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings("test-key", mock.BaseAddress + "/", "mock-model"),
        });

        var scoped = ServerWideConnectionString.GetDatabaseRecordConnectionStringName(connectionStringName);

        const string agentId = "support";
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Support",
            SystemPrompt = "You help.",
            ConnectionStringName = scoped,
        });

        var channel = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, agentId, ["http://localhost"]));

        var minted = await app.MintEmbedLinkAsync(
            new MintEmbedLinkRequest(channel.ChannelId, null, TtlSeconds: 3600, MaxInvocations: 50));

        return new Harness(app, minted.Token, scoped);
    }
}
