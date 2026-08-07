using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;
using static QuillTests.E2E.Fixtures.ActionFixtures;

namespace QuillTests;

[Collection(QuillAgentActionsCollection.Name)]
public class AgentActionE2ETests(ITestOutputHelper output, QuillCollectionHost collection)
    : QuillTestBase(output, collection)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Webhook_action_runs_inside_the_turn_and_its_response_reaches_the_model()
    {
        await using var mock = await MockQuillServices.StartAsync(
            new ToolCallTurn("create_ticket", """{"subject":"Broken"}""", ToolId: "call_1"),
            new FinalTurn("""{"reply":"Ticket filed"}"""));
        mock.WebhookResponse = (200, """{"ticketId":"T-1"}""");

        await using var h = await HarnessAsync(mock, Webhook(mock.WebhookUrl, secret: "s3cret"));

        var ndjson = await h.App.SendEmbedChatAsync(h.Token, "my laptop is broken");

        Assert.Equal("Ticket filed", ReplyOf(ndjson));

        var delivery = Assert.Single(mock.Deliveries);
        Assert.Equal("s3cret", delivery.Headers["X-Quill-Secret"]);

        // the body is the model's arguments, and userId is in there because the server merges the
        // conversation's parameters over anything the model supplied — the model cannot forge it
        Assert.Equal("""{"subject":"Broken","userId":"users/42"}""", delivery.Body.GetRawText());

        Assert.Contains("T-1", mock.LastToolMessageContent());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Webhook_failure_reaches_the_model_and_the_turn_still_completes()
    {
        await using var mock = await MockQuillServices.StartAsync(
            new ToolCallTurn("create_ticket", """{"subject":"Broken"}""", ToolId: "call_1"),
            new FinalTurn("""{"reply":"Sorry, I could not file that"}"""));
        mock.WebhookResponse = (500, """{"error":"boom"}""");

        await using var h = await HarnessAsync(mock, Webhook(mock.WebhookUrl));

        var ndjson = await h.App.SendEmbedChatAsync(h.Token, "my laptop is broken");

        Assert.Equal("Sorry, I could not file that", ReplyOf(ndjson));

        var toolMessage = mock.LastToolMessageContent();
        Assert.StartsWith("action failed: webhook returned 500", toolMessage);
        Assert.EndsWith("""{"error":"boom"}""", toolMessage);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_binding_keyed_with_different_casing_still_resolves()
    {
        await using var mock = await MockQuillServices.StartAsync(
            new ToolCallTurn("create_ticket", """{"subject":"Broken"}""", ToolId: "call_1"),
            new FinalTurn("""{"reply":"Ticket filed"}"""));

        // the sidecar's dictionary is case-insensitive in memory; this proves the comparer survives
        // the RavenDB round-trip, since a case-sensitive lookup would answer "no binding configured"
        await using var h = await HarnessAsync(mock, Webhook(mock.WebhookUrl), bindingKey: "CREATE_TICKET");

        var ndjson = await h.App.SendEmbedChatAsync(h.Token, "my laptop is broken");

        Assert.Equal("Ticket filed", ReplyOf(ndjson));
        Assert.Single(mock.Deliveries);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_aborted_turn_redelivers_the_action_on_the_next_turn()
    {
        await using var mock = await MockQuillServices.StartAsync(
            new ToolCallTurn("create_ticket", """{"subject":"Broken"}""", ToolId: "call_1"),
            new FinalTurn("""{"reply":"Ticket filed"}"""));
        mock.WebhookDelay = TimeSpan.FromMinutes(2);

        await using var h = await HarnessAsync(mock, Webhook(mock.WebhookUrl));

        await AbortMidActionAsync(h, mock);

        mock.WebhookDelay = TimeSpan.Zero;
        await h.App.SendEmbedChatAsync(h.Token, "any news?");

        // delivery is at-least-once and the body carries no tool id, so the receiver has nothing to
        // dedupe on: the same action arrives twice, indistinguishably
        Assert.Equal(2, mock.Deliveries.Count);
        Assert.Equal(mock.Deliveries[0].Body.GetRawText(), mock.Deliveries[1].Body.GetRawText());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_pending_action_whose_binding_vanished_fails_gracefully()
    {
        await using var mock = await MockQuillServices.StartAsync(
            new ToolCallTurn("create_ticket", """{"subject":"Broken"}""", ToolId: "call_1"),
            new FinalTurn("""{"reply":"I could not file that"}"""));
        mock.WebhookDelay = TimeSpan.FromMinutes(2);

        await using var h = await HarnessAsync(mock, Webhook(mock.WebhookUrl));

        await AbortMidActionAsync(h, mock);

        // raw delete: validation makes an action without its binding unreachable through the API
        using (var session = h.App.Store.OpenAsyncSession(h.App.Slug))
        {
            session.Delete(AgentActionBindings.IdFor(h.AgentId));
            await session.SaveChangesAsync();
        }

        var ndjson = await h.App.SendEmbedChatAsync(h.Token, "any news?");

        Assert.Equal("I could not file that", ReplyOf(ndjson));
        Assert.Equal("action failed: no binding configured for 'create_ticket'", mock.LastToolMessageContent());
        Assert.Single(mock.Deliveries);
    }

    /// Starts a turn, waits for the action to reach the receiver, then aborts the client mid-flight —
    /// leaving the conversation with a pending action for the next turn to recover.
    private static async Task AbortMidActionAsync(Harness h, MockQuillServices mock)
    {
        using var abort = new CancellationTokenSource();
        var aborted = h.App.SendEmbedChatAsync(h.Token, "my laptop is broken", ct: abort.Token);

        await mock.WaitForDeliveriesAsync(1, TimeSpan.FromSeconds(30));
        await abort.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => aborted);
    }

    /// <param name="mock">Serves both roles: the LLM RavenDB talks to, and the action webhook.</param>
    private async Task<Harness> HarnessAsync(
        MockQuillServices mock, WebhookBinding binding, string bindingKey = "create_ticket")
    {
        var app = await NewAppAsync();

        var connectionStringName = "mock-llm-" + Guid.NewGuid().ToString("N");
        await Host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = connectionStringName,
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings("test-key", mock.BaseAddress + "/", "mock-model"),
        });

        const string agentId = "support";
        await app.ProvisionAgentAsync(new EditAgentRequest(
            new AiAgentConfiguration
            {
                Identifier = agentId,
                Name = "Support",
                SystemPrompt = "You file support tickets for the current user.",
                ConnectionStringName =
                    ServerWideConnectionString.GetDatabaseRecordConnectionStringName(connectionStringName),
                Actions =
                [
                    new AiAgentToolAction("create_ticket", "Files a support ticket.")
                    {
                        ParametersSampleObject = """{"subject":"printer is jammed"}""",
                    },
                ],
                Parameters = [new AiAgentParameter { Name = "userId", Description = "The current user." }],
            },
            new Dictionary<string, WebhookBinding> { [bindingKey] = binding }));

        var channel = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, agentId, []));

        var minted = await app.MintEmbedLinkAsync(new MintEmbedLinkRequest(
            channel.ChannelId,
            new Dictionary<string, string> { ["userId"] = "users/42" },
            TtlSeconds: 3600,
            MaxInvocations: 50));

        return new Harness(app, minted.Token, agentId);
    }

    private static string? ReplyOf(string ndjson)
    {
        foreach (var line in ndjson.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            using var frame = JsonDocument.Parse(line);
            if (frame.RootElement.GetProperty("type").GetString() != "done")
                continue;

            return frame.RootElement.GetProperty("answer").GetProperty("reply").GetString();
        }

        throw new InvalidOperationException($"no 'done' frame in the NDJSON response: {ndjson}");
    }

    // the mock is owned by the test, not the harness — it is created before the agent so its address
    // can go into the connection string
    private sealed record Harness(QuillApp App, string Token, string AgentId) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => App.DisposeAsync();
    }
}
