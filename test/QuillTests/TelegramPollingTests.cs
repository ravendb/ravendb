using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Metrics;
using Raven.Quill.Telegram;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillTelegramCollection.Name)]
public class TelegramPollingTests(ITestOutputHelper output, QuillTelegramFixture fixture)
    : QuillTelegramTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Disable_stops_polling_and_reenable_resumes()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(token) >= 1, "polling to start");

        await app.UpdateChannelAsync(channelId, new UpdateChannelRequest(null, null, Enabled: false));
        var frozen = await WaitForPollingToSettleAsync(token);
        await Task.Delay(700);
        Assert.Equal(frozen, Mock.GetUpdatesCallCount(token));

        await app.UpdateChannelAsync(channelId, new UpdateChannelRequest(null, null, Enabled: true));
        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(token) > frozen, "polling to resume");

        await app.DeleteChannelAsync(channelId);
        var afterDelete = await WaitForPollingToSettleAsync(token);
        await Task.Delay(700);
        Assert.Equal(afterDelete, Mock.GetUpdatesCallCount(token));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Rotating_the_bot_token_moves_polling_to_the_new_token()
    {
        var (app, channelId, oldToken) = await ProvisionAsync();
        await using var appGuard = app;

        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(oldToken) >= 1, "polling to start");

        var newToken = NewBotToken();
        await app.UpdateChannelAsync(channelId, new UpdateChannelRequest(null, null, null, BotToken: newToken));

        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(newToken) >= 1, "polling on the new token");

        var frozen = await WaitForPollingToSettleAsync(oldToken);
        await Task.Delay(700);
        Assert.Equal(frozen, Mock.GetUpdatesCallCount(oldToken));

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Deleting_one_app_leaves_another_apps_bot_polling()
    {
        var (keptApp, keptChannelId, keptToken) = await ProvisionAsync();
        await using var keptGuard = keptApp;
        var (doomedApp, _, doomedToken) = await ProvisionAsync();

        await Mock.WaitUntilAsync(
            () => Mock.GetUpdatesCallCount(keptToken) >= 1 && Mock.GetUpdatesCallCount(doomedToken) >= 1,
            "both bots to start");

        await doomedApp.Host.DeleteAppAsync(doomedApp.Slug);
        await doomedApp.DisposeAsync();

        // the deleted app's database is gone, which a pass must treat as a definitive answer for that app
        // only - the surviving app's bot keeps polling
        var doomedFrozen = await WaitForPollingToSettleAsync(doomedToken);
        var keptBefore = Mock.GetUpdatesCallCount(keptToken);

        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(keptToken) > keptBefore, "the surviving bot to poll on");
        Assert.Equal(doomedFrozen, Mock.GetUpdatesCallCount(doomedToken));

        await keptApp.DeleteChannelAsync(keptChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Message_dispatches_with_derived_conversation_id_and_bound_parameters()
    {
        var (app, channelId, token) = await ProvisionAsync(
            parameters: new Dictionary<string, string> { ["customerId"] = "customers/42" },
            declared:
            [
                new AiAgentParameter("customerId", "scope"),
                new AiAgentParameter("telegramUserIdentifier", "telegram sender"),
            ]);
        await using var appGuard = app;

        Mock.EnqueueTextMessage(token, chatId: 555, fromUserId: 777, "hello there");
        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 1, "the agent run");

        var request = Assert.Single(Router.Requests);
        Assert.Equal(app.Slug, request.Database);
        Assert.Equal(Channel.IdPrefix + channelId, request.ChannelId);
        Assert.Equal("hello there", request.Prompt);
        Assert.StartsWith($"chats/tg/{channelId}/555/", request.ConversationId);
        Assert.True(AgentRouter.TryNormalizeConversationId(request.ConversationId, out _, out _));
        Assert.Equal("customers/42", request.Parameters["customerId"]);
        Assert.Equal("777", request.Parameters["telegramUserIdentifier"]);

        await Mock.WaitUntilAsync(() => Mock.SentMessages.Any(m => m.ChatId == 555), "the reply");
        var typing = Assert.Single(Mock.ChatActions, a => a.ChatId == 555);
        Assert.Equal("typing", typing.Action);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Telegram_username_is_injected_under_the_declared_casing()
    {
        var (app, channelId, token) = await ProvisionAsync(declared:
        [
            new AiAgentParameter("telegramUserName", "sender's handle"),
            new AiAgentParameter("telegramUserIdentifier", "telegram sender"),
        ]);
        await using var appGuard = app;

        Mock.EnqueueTextMessage(token, chatId: 500, fromUserId: 501, "hello", username: "Alice_42");
        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 1, "the agent run");

        var request = Assert.Single(Router.Requests);
        Assert.Equal("Alice_42", request.Parameters["telegramUserName"]);
        Assert.Equal("501", request.Parameters["telegramUserIdentifier"]);

        await Mock.WaitUntilAsync(() => Mock.SentMessages.Any(m => m.ChatId == 500), "the reply");

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Missing_telegram_username_sends_a_canned_nudge_and_skips_the_agent()
    {
        var (app, channelId, token) = await ProvisionAsync(declared:
        [
            new AiAgentParameter("TelegramUsername", "sender's handle"),
        ]);
        await using var appGuard = app;

        const long chatId = 510;
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 510, "first try");
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 510, "second try");

        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Count(m => m.ChatId == chatId && m.Text.Contains("username")) == 2,
            "the nudges");

        Assert.All(Mock.SentMessages.Where(m => m.ChatId == chatId), m => Assert.Null(m.ParseMode));
        Assert.Empty(Router.Requests);
        Assert.DoesNotContain(Mock.ChatActions, a => a.ChatId == chatId);

        var conversationId = TelegramConversationId.For(channelId, chatId, DateTime.UtcNow);
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            Assert.Null(await session.LoadAsync<object>(conversationId));
            Assert.Null(await session.LoadAsync<object>(ConversationPreview.IdFor(conversationId)));
        }

        var health = Assert.Single(await app.GetTelegramHealthAsync());
        Assert.Equal(0, health.ErrorCount);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Parameter_added_after_provisioning_is_refused_before_the_agent_runs()
    {
        var (app, channelId, token) = await ProvisionAsync(
            parameters: new Dictionary<string, string> { ["customerId"] = "customers/42" },
            declared: [new AiAgentParameter("customerId", "scope")]);
        await using var appGuard = app;

        var agentId = (await app.GetAgentsAsync()).Single().AgentId;
        var config = await app.GetAgentAsync(agentId);
        config.Parameters.Add(new AiAgentParameter("region", "added after the channel was provisioned"));
        await app.EditAgentAsync(config);

        const long chatId = 530;
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 530, "hello");
        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text.Contains("not fully configured")),
            "the misconfiguration notice");

        Assert.Empty(Router.Requests);
        Assert.DoesNotContain(Mock.ChatActions, a => a.ChatId == chatId);

        await Mock.WaitUntilAsync(
            () => app.GetTelegramHealthAsync().Result.Single().ErrorCount >= 1, "the health error");
        var health = Assert.Single(await app.GetTelegramHealthAsync());
        Assert.Contains("region", health.LastError);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Processed_updates_are_confirmed_and_never_redelivered()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(token) >= 1, "polling to start");
        var before = Mock.LastGetUpdatesOffset(token) ?? 0;

        Mock.EnqueueTextMessage(token, chatId: 1, fromUserId: 1, "first");
        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 1, "the first run");

        // the receiver starts from offset 0, so confirmation shows up as the offset advancing past it
        await Mock.WaitUntilAsync(
            () => Mock.LastGetUpdatesOffset(token) > before, "the poll that confirms the first update");

        Mock.EnqueueTextMessage(token, chatId: 1, fromUserId: 1, "second");
        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 2, "the second run");

        Assert.Equal(new[] { "first", "second" }, Router.Requests.Select(r => r.Prompt).ToArray());

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Streamed_reply_lands_as_one_message_edited_in_place()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        Router.Chunks = ["The answer ", "is 42. ", "No doubt about it."];
        Router.ChunkDelay = TimeSpan.FromMilliseconds(120);

        Mock.EnqueueTextMessage(token, chatId: 7, fromUserId: 7, "question");
        var full = string.Concat(Router.Chunks);
        await Mock.WaitUntilAsync(
            () => Mock.EditedMessages.Any(e => e.ChatId == 7 && e.Text == full), "the final edit");

        var sent = Assert.Single(Mock.SentMessages, m => m.ChatId == 7);
        Assert.All(Mock.EditedMessages.Where(e => e.ChatId == 7), e => Assert.Equal(sent.MessageId, e.MessageId));

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Markdown_rejection_falls_back_to_plain_text()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        Mock.FailParseModeRequests = true;
        Router.Chunks = ["*broken _markdown"];

        Mock.EnqueueTextMessage(token, chatId: 9, fromUserId: 9, "hi");
        await Mock.WaitUntilAsync(() => Mock.SentMessages.Any(m => m.ChatId == 9), "the plain-text retry");

        Assert.All(Mock.SentMessages.Where(m => m.ChatId == 9), m => Assert.Null(m.ParseMode));

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Long_reply_splits_at_a_sentence_boundary_into_follow_up_messages()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        var reply = string.Join(" ", Enumerable.Range(0, 160).Select(i => $"Sentence number {i} is here."));
        Assert.True(reply.Length > 4096);
        Router.Chunks = [reply];

        var parts = TelegramMessageSplitter.Split(reply);
        Assert.Equal(2, parts.Count);

        Mock.EnqueueTextMessage(token, chatId: 11, fromUserId: 11, "long please");
        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Count(m => m.ChatId == 11) == 2, "the overflow follow-up message");

        var sends = Mock.SentMessages.Where(m => m.ChatId == 11).ToArray();
        Assert.Equal(parts[1], sends[1].Text);
        await Mock.WaitUntilAsync(
            () => Mock.EditedMessages.Any(e => e.ChatId == 11 && e.Text == parts[0]), "the boundary edit");

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Turns_are_serialized_per_chat_and_concurrent_across_chats()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Router.BeforeRun = request => request.Prompt == "a1" ? gate.Task : Task.CompletedTask;

        Mock.EnqueueTextMessage(token, chatId: 100, fromUserId: 100, "a1");
        await Mock.WaitUntilAsync(() => Router.Requests.Any(r => r.Prompt == "a1"), "chat A's first run to start");

        Mock.EnqueueTextMessage(token, chatId: 100, fromUserId: 100, "a2");
        Mock.EnqueueTextMessage(token, chatId: 200, fromUserId: 200, "b1");

        await Mock.WaitUntilAsync(() => Router.Requests.Any(r => r.Prompt == "b1"), "chat B's run");
        await Task.Delay(300);
        Assert.DoesNotContain(Router.Requests, r => r.Prompt == "a2");

        gate.SetResult();
        await Mock.WaitUntilAsync(() => Router.Requests.Any(r => r.Prompt == "a2"), "chat A's second run");

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_flooded_chat_sheds_messages_and_warns_the_sender_once()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        const long chatId = 610;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Router.BeforeRun = request => request.Prompt == "m0" ? gate.Task : Task.CompletedTask;

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 610, "m0");
        await Mock.WaitUntilAsync(() => Router.Requests.Any(r => r.Prompt == "m0"), "the blocking turn to start");

        // one turn in flight plus a queue of TelegramChatQueueCapacity, so the tail cannot be admitted
        for (var i = 1; i <= 12; i++)
            Mock.EnqueueTextMessage(token, chatId, fromUserId: 610, $"m{i}");

        await Mock.WaitUntilAsync(
            () => app.GetTelegramHealthAsync().Result.Single().ErrorCount >= 1, "the shed message");
        var health = Assert.Single(await app.GetTelegramHealthAsync());
        Assert.Contains("queue full", health.LastError);

        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text.Contains("didn't make it")),
            "the overload warning");
        await Task.Delay(400);

        // one warning for the saturation episode, not one per dropped message
        var warning = Assert.Single(Mock.SentMessages, m => m.ChatId == chatId && m.Text.Contains("didn't make it"));
        Assert.Null(warning.ParseMode);

        gate.SetResult();

        // whatever was admitted still runs, in arrival order
        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 2, "the queued turns");
        var prompts = Router.Requests.Select(r => int.Parse(r.Prompt[1..])).ToArray();
        Assert.Equal(prompts.Order().ToArray(), prompts);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Clear_command_deletes_the_conversation_and_its_preview()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        const long chatId = 300;
        var conversationId = TelegramConversationId.For(channelId, chatId, DateTime.UtcNow);
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            await session.StoreAsync(new ConversationPreview { ConversationId = conversationId },
                ConversationPreview.IdFor(conversationId));
            await session.StoreAsync(new { Seeded = true }, conversationId);
            await session.SaveChangesAsync();
        }

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 300, "/clear");
        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text.Contains("cleared")), "the confirmation");

        Assert.Empty(Router.Requests);
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            Assert.Null(await session.LoadAsync<object>(conversationId));
            Assert.Null(await session.LoadAsync<object>(ConversationPreview.IdFor(conversationId)));
        }

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_command_sends_a_canned_greeting_and_skips_the_agent()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        Mock.EnqueueTextMessage(token, chatId: 310, fromUserId: 310, "/start");
        Mock.EnqueueTextMessage(token, chatId: 311, fromUserId: 311, "/start@quill_test_bot deep-link-payload");

        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == 310 && m.Text.Contains("Ask me anything")) &&
                  Mock.SentMessages.Any(m => m.ChatId == 311 && m.Text.Contains("Ask me anything")),
            "the greetings");

        Assert.Empty(Router.Requests);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Group_chat_messages_get_a_canned_refusal_including_commands()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        const long chatId = -520;
        var conversationId = TelegramConversationId.For(channelId, chatId, DateTime.UtcNow);
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            await session.StoreAsync(new ConversationPreview { ConversationId = conversationId },
                ConversationPreview.IdFor(conversationId));
            await session.StoreAsync(new { Seeded = true }, conversationId);
            await session.SaveChangesAsync();
        }

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 520, "hello group", chatType: "group");
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 520, "/clear", chatType: "supergroup");
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 520, "/start", chatType: "channel");

        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Count(m => m.ChatId == chatId && m.Text.Contains("one-on-one")) == 3,
            "the refusals");

        Assert.All(Mock.SentMessages.Where(m => m.ChatId == chatId), m => Assert.Null(m.ParseMode));
        Assert.Empty(Router.Requests);
        Assert.DoesNotContain(Mock.SentMessages, m => m.ChatId == chatId && m.Text.Contains("Ask me anything"));

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            Assert.NotNull(await session.LoadAsync<object>(conversationId));
            Assert.NotNull(await session.LoadAsync<object>(ConversationPreview.IdFor(conversationId)));
        }

        var health = Assert.Single(await app.GetTelegramHealthAsync());
        Assert.Equal(0, health.ErrorCount);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agent_failure_sends_an_apology_and_keeps_the_poller_alive()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        Router.Failure = new InvalidOperationException("model exploded");
        Mock.EnqueueTextMessage(token, chatId: 400, fromUserId: 400, "boom");
        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == 400 && m.Text.StartsWith("Sorry")), "the apology");

        Router.Failure = null;
        Mock.EnqueueTextMessage(token, chatId: 400, fromUserId: 400, "again");
        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == 400 && m.Text.Contains("fake agent")), "the recovery reply");

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Health_reports_polls_errors_and_recovery()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(token) >= 1, "polling to start");

        var healthy = Assert.Single(await app.GetTelegramHealthAsync());
        Assert.Equal(channelId, healthy.ChannelId);
        Assert.Equal("quill_test_bot", healthy.BotUsername);
        Assert.True(healthy.Enabled);
        Assert.True(healthy.IsPolling);

        Mock.GetUpdatesFailure = MockTelegramBotApi.Unauthorized;
        await Mock.WaitUntilAsync(
            () => app.GetTelegramHealthAsync().Result.Single().ErrorCount >= 1, "the error counter");
        var failing = Assert.Single(await app.GetTelegramHealthAsync());
        Assert.NotNull(failing.LastErrorAt);
        Assert.Contains("Unauthorized", failing.LastError);

        Mock.GetUpdatesFailure = null;
        await Mock.WaitUntilAsync(
            () => app.GetTelegramHealthAsync().Result.Single() is { LastSuccessfulPoll: not null } h &&
                  h.LastSuccessfulPoll > failing.LastErrorAt,
            "a successful poll after recovery");

        await app.UpdateChannelAsync(channelId, new UpdateChannelRequest(null, null, Enabled: false));
        await Mock.WaitUntilAsync(
            () => app.GetTelegramHealthAsync().Result.Single().IsPolling == false, "the bot to stop");
        var disabled = Assert.Single(await app.GetTelegramHealthAsync());
        Assert.False(disabled.Enabled);

        await app.DeleteChannelAsync(channelId);
        Assert.Empty(await app.GetTelegramHealthAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Health_returns_404_for_unknown_slug()
    {
        var e = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetTelegramHealthAsync("no-such-app"));
        Assert.Equal(System.Net.HttpStatusCode.NotFound, e.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task App_delete_stops_the_app_pollers()
    {
        var (app, _, token) = await ProvisionAsync();
        await using var appGuard = app;

        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(token) >= 1, "polling to start");

        await app.Host.DeleteAppAsync(app.Slug);
        var frozen = await WaitForPollingToSettleAsync(token);
        await Task.Delay(700);
        Assert.Equal(frozen, Mock.GetUpdatesCallCount(token));
    }

    private async Task<(QuillApp App, string ChannelId, string Token)> ProvisionAsync(
        Dictionary<string, string>? parameters = null, AiAgentParameter[]? declared = null)
    {
        var app = await NewAppAsync();
        var agentId = "tg-agent-" + Guid.NewGuid().ToString("N")[..8];
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Telegram Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Parameters = (declared ?? []).ToList(),
        });

        var token = NewBotToken();
        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Telegram, agentId, null, BotToken: token, Parameters: parameters));

        return (app, created.ChannelId, token);
    }
}
