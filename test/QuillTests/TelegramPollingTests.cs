using Microsoft.Extensions.DependencyInjection;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Hosting;
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
        await app.UpdateChannelAsync(channelId, new UpdateChannelRequest(null, null, null, new(newToken)));

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
            bindings: new Dictionary<string, ChannelParameterBinding>
            {
                ["customerId"] = new() { Source = ChannelParameterSource.Constant, Value = "customers/42" },
                ["senderId"] = new() { Source = ChannelParameterSource.UserId },
            },
            declared:
            [
                new AiAgentParameter("customerId", "scope"),
                new AiAgentParameter("senderId", "telegram sender"),
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
        Assert.Equal("customers/42", request.Parameters["customerId"].GetString());
        Assert.Equal("777", request.Parameters["senderId"].GetString());

        await Mock.WaitUntilAsync(() => Mock.SentMessages.Any(m => m.ChatId == 555), "the reply");
        var typing = Assert.Single(Mock.ChatActions, a => a.ChatId == 555);
        Assert.Equal("typing", typing.Action);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Sender_values_are_injected_under_the_declared_parameter_casing()
    {
        var (app, channelId, token) = await ProvisionAsync(
            bindings: new Dictionary<string, ChannelParameterBinding>
            {
                ["USERHANDLE"] = new() { Source = ChannelParameterSource.Username },
                ["SENDERID"] = new() { Source = ChannelParameterSource.UserId },
            },
            declared:
            [
                new AiAgentParameter("userHandle", "sender's handle"),
                new AiAgentParameter("senderId", "telegram sender"),
            ]);
        await using var appGuard = app;

        Mock.EnqueueTextMessage(token, chatId: 500, fromUserId: 501, "hello", username: "Alice_42");
        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 1, "the agent run");

        var request = Assert.Single(Router.Requests);
        Assert.Equal("Alice_42", request.Parameters["userHandle"].GetString());
        Assert.Equal("501", request.Parameters["senderId"].GetString());

        await Mock.WaitUntilAsync(() => Mock.SentMessages.Any(m => m.ChatId == 500), "the reply");

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Missing_telegram_username_sends_a_canned_nudge_and_skips_the_agent()
    {
        var (app, channelId, token) = await ProvisionAsync(
            bindings: new Dictionary<string, ChannelParameterBinding>
            {
                ["handle"] = new() { Source = ChannelParameterSource.Username },
            },
            declared:
            [
                new AiAgentParameter("handle", "sender's handle"),
            ]);
        await using var appGuard = app;

        const long chatId = 510;
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 510, "first try");
        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text.Contains("username")), "the first nudge");

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 510, "second try");
        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Count(m => m.ChatId == chatId && m.Text.Contains("username")) == 2,
            "the second nudge");

        Assert.All(Mock.SentMessages.Where(m => m.ChatId == chatId), m => Assert.Null(m.ParseMode));
        Assert.Empty(Router.Requests);
        Assert.DoesNotContain(Mock.ChatActions, a => a.ChatId == chatId);

        var conversationId = TelegramConversationId.ForUtcDay(channelId, chatId, DateTime.UtcNow);
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            Assert.Null(await session.LoadAsync<object>(conversationId));
            Assert.Null(await session.LoadAsync<object>(ConversationPreview.IdFor(conversationId)));
        }

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Parameter_added_after_provisioning_is_refused_before_the_agent_runs()
    {
        var (app, channelId, token) = await ProvisionAsync(
            bindings: new Dictionary<string, ChannelParameterBinding>
            {
                ["customerId"] = new() { Source = ChannelParameterSource.Constant, Value = "customers/42" },
            },
            declared: [new AiAgentParameter("customerId", "scope")]);
        await using var appGuard = app;

        var agentId = (await app.GetAgentsAsync()).Single().AgentId;
        var details = await app.GetAgentAsync(agentId);
        details.Configuration.Parameters.Add(new AiAgentParameter("region", "added after the channel was provisioned"));
        await app.EditAgentAsync(new EditAgentRequest(details.Configuration, details.ActionBindings));

        const long chatId = 530;
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 530, "hello");
        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text.Contains("not fully configured")),
            "the misconfiguration notice");

        Assert.Empty(Router.Requests);
        Assert.DoesNotContain(Mock.ChatActions, a => a.ChatId == chatId);

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
    public async Task Unformatted_reply_already_shown_by_the_preview_finalizes_without_an_error()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        const long chatId = 15;
        Router.Chunks = ["A short answer with no formatting."];

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 15, "hi");
        await Mock.WaitUntilAsync(() => Mock.SentMessages.Any(m => m.ChatId == chatId), "the preview send");

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 15, "again");
        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 2, "the second turn");

        Assert.DoesNotContain(Mock.SentMessages, m => m.ChatId == chatId && m.Text.Contains("went wrong"));

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Long_reply_splits_at_a_sentence_boundary_into_follow_up_messages()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        var reply = string.Join(" ", Enumerable.Range(0, 160).Select(i => $"Sentence number {i} is here.")) + " The *end* of it.";
        Assert.True(reply.Length > 4096);
        Router.Chunks = [reply];

        var parts = MessageSplitter.Split(reply, TelegramOptions.ApiMessageLimit);
        Assert.Equal(2, parts.Count);

        Mock.EnqueueTextMessage(token, chatId: 11, fromUserId: 11, "long please");
        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Count(m => m.ChatId == 11) == 2, "the rolled preview messages");

        Assert.Contains(Mock.SentMessages, m => m.ChatId == 11 && m.Text == parts[0]);

        await Mock.WaitUntilAsync(
            () => Mock.EditedMessages.Any(e => e.ChatId == 11 && e.Text == parts[1]), "the finalize edit of the tail");

        Assert.DoesNotContain(Mock.EditedMessages, e => e.ChatId == 11 && e.Text == parts[0]);

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
    public async Task Messages_queued_behind_a_running_turn_are_merged_into_one_prompt()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        const long chatId = 150;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Router.BeforeRun = request => request.Prompt == "hold" ? gate.Task : Task.CompletedTask;

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 150, "hold");
        await Mock.WaitUntilAsync(() => Router.Requests.Any(r => r.Prompt == "hold"), "the blocking turn to start");

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 150, "what is the status");
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 150, "of order 42");
        await Mock.WaitUntilAsync(
            () => Mock.PendingUpdateCount(token) == 0, "the queued updates to reach the blocked chat");

        gate.SetResult();
        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 2, "the merged turn");
        await Task.Delay(400);

        Assert.Equal(
            ["hold", "what is the status\nof order 42"],
            Router.Requests.Select(r => r.Prompt).ToArray());

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_command_queued_between_messages_is_not_merged_into_the_prompt()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        const long chatId = 160;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Router.BeforeRun = request => request.Prompt == "hold" ? gate.Task : Task.CompletedTask;

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 160, "hold");
        await Mock.WaitUntilAsync(() => Router.Requests.Any(r => r.Prompt == "hold"), "the blocking turn to start");

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 160, "before");
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 160, "/start");
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 160, "after");
        await Mock.WaitUntilAsync(
            () => Mock.PendingUpdateCount(token) == 0, "the queued updates to reach the blocked chat");

        gate.SetResult();
        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 3, "both merged turns");
        await Task.Delay(400);

        Assert.Equal(["hold", "before", "after"], Router.Requests.Select(r => r.Prompt).ToArray());
        Assert.Contains(Mock.SentMessages, m => m.ChatId == chatId && m.Text.Contains("Ask me anything"));

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

        for (var i = 1; i <= 12; i++)
            Mock.EnqueueTextMessage(token, chatId, fromUserId: 610, $"m{i}");

        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text.Contains("didn't make it")),
            "the overload warning");
        await Task.Delay(400);

        var warning = Assert.Single(Mock.SentMessages, m => m.ChatId == chatId && m.Text.Contains("didn't make it"));
        Assert.Null(warning.ParseMode);

        gate.SetResult();

        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 2, "the queued turns");
        var prompts = Router.Requests
            .SelectMany(r => r.Prompt.Split('\n'))
            .Select(p => int.Parse(p[1..]))
            .ToArray();
        Assert.Equal(prompts.Order().ToArray(), prompts);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Idle_chat_is_evicted_and_the_next_message_revives_it()
    {
        await using var host = await NewHostAsync(configure: opts =>
            opts.Telegram.ChatIdleTimeout = TimeSpan.FromMilliseconds(700));
        var app = await NewAppAsync(host);
        await using var appGuard = app;

        var agentId = "tg-agent-" + Guid.NewGuid().ToString("N")[..8];
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Telegram Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        var token = NewBotToken();
        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Telegram, agentId, null, Telegram: new(token)));
        var channelId = created.ChannelId;

        var manager = host.Services.GetRequiredService<TelegramChannelManager>();

        const long chatId = 800;
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 800, "first");
        await Mock.WaitUntilAsync(
            () => manager.GetActiveChatCount(app.Slug, channelId) == 1, "the chat worker");

        await Mock.WaitUntilAsync(
            () => manager.GetActiveChatCount(app.Slug, channelId) == 0, "the idle eviction");

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 800, "second");
        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 2, "the revived chat's run");
        Assert.Equal(new[] { "first", "second" }, Router.Requests.Select(r => r.Prompt).ToArray());
        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Count(m => m.ChatId == chatId) >= 2, "the second reply");

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_channel_doc_that_cannot_start_does_not_starve_other_channels()
    {
        await using var host = await NewHostAsync();
        var app = await NewAppAsync(host);
        await using var appGuard = app;

        // sorts before every provisioned channel, so the apply pass visits it first
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            await session.StoreAsync(new Channel
            {
                Id = Channel.IdPrefix + "!poisoned",
                Type = ChannelType.Telegram,
                AgentId = "missing",
                Enabled = true,
                CreatedAt = DateTime.UtcNow,
                Telegram = new TelegramSettings { BotToken = "not-a-token" },
            });
            await session.SaveChangesAsync();
        }

        var agentId = "tg-agent-" + Guid.NewGuid().ToString("N")[..8];
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Telegram Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        var token = NewBotToken();
        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Telegram, agentId, null, Telegram: new(token)));

        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(token) >= 1, "the healthy channel's poller");

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Message_overrides_apply_to_a_running_bot()
    {
        var (app, channelId, token) = await ProvisionAsync(
            bindings: new Dictionary<string, ChannelParameterBinding>
            {
                ["handle"] = new() { Source = ChannelParameterSource.Username },
            },
            declared: [new AiAgentParameter("handle", "sender's handle")]);
        await using var appGuard = app;

        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(token) >= 1, "polling to start");

        await app.UpdateChannelAsync(channelId, new UpdateChannelRequest(null, null, null,
            new(Messages: new TelegramChannelMessages
            {
                Greeting = "Witaj! Zadaj mi pytanie.",
                UsernameMissing = "Ustaw nazwe uzytkownika w Telegramie i sprobuj ponownie.",
            })));

        // a /start may still land on the outgoing bot until the manager swaps runtimes,
        // so keep nudging until the new greeting appears
        const long chatId = 620;
        for (var attempt = 0; attempt < 40; attempt++)
        {
            Mock.EnqueueTextMessage(token, chatId, fromUserId: 620, "/start");
            await Task.Delay(250);
            if (Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text == "Witaj! Zadaj mi pytanie."))
                break;
        }
        Assert.Contains(Mock.SentMessages, m => m.ChatId == chatId && m.Text == "Witaj! Zadaj mi pytanie.");

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 620, "hello");
        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text.StartsWith("Ustaw nazwe")),
            "the overridden username nudge");
        Assert.Empty(Router.Requests);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Clear_command_deletes_the_conversation_and_its_preview()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        const long chatId = 300;
        var conversationId = TelegramConversationId.ForUtcDay(channelId, chatId, DateTime.UtcNow);
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
    public async Task Start_command_nudges_for_a_missing_username_without_waiting_for_a_first_message()
    {
        var (app, channelId, token) = await ProvisionAsync(
            bindings: new Dictionary<string, ChannelParameterBinding>
            {
                ["userHandle"] = new() { Source = ChannelParameterSource.Username },
            },
            declared: [new AiAgentParameter("userHandle", "sender's handle")]);
        await using var appGuard = app;

        const long chatId = 320;
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 320, "/start");

        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text.Contains("needs your Telegram username")),
            "the username nudge");
        Assert.Contains(Mock.SentMessages, m => m.ChatId == chatId && m.Text.Contains("Ask me anything"));
        Assert.Empty(Router.Requests);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Start_command_stays_quiet_when_every_binding_can_be_satisfied()
    {
        var (app, channelId, token) = await ProvisionAsync(
            bindings: new Dictionary<string, ChannelParameterBinding>
            {
                ["userHandle"] = new() { Source = ChannelParameterSource.Username },
            },
            declared: [new AiAgentParameter("userHandle", "sender's handle")]);
        await using var appGuard = app;

        const long chatId = 321;
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 321, "/start", username: "alice");

        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text.Contains("Ask me anything")),
            "the greeting");

        Assert.DoesNotContain(Mock.SentMessages, m => m.ChatId == chatId && m.Text.Contains("needs your Telegram"));
        Assert.Empty(Router.Requests);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Group_chat_messages_get_one_canned_refusal_per_chat_including_commands()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        const long groupChatId = -520;
        const long supergroupChatId = -521;
        const long channelChatId = -522;
        var conversationId = TelegramConversationId.ForUtcDay(channelId, supergroupChatId, DateTime.UtcNow);
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            await session.StoreAsync(new ConversationPreview { ConversationId = conversationId },
                ConversationPreview.IdFor(conversationId));
            await session.StoreAsync(new { Seeded = true }, conversationId);
            await session.SaveChangesAsync();
        }

        Mock.EnqueueTextMessage(token, groupChatId, fromUserId: 520, "hello group", chatType: "group");
        Mock.EnqueueTextMessage(token, supergroupChatId, fromUserId: 520, "/clear", chatType: "supergroup");
        Mock.EnqueueTextMessage(token, channelChatId, fromUserId: 520, "/start", chatType: "channel");

        long[] chatIds = [groupChatId, supergroupChatId, channelChatId];
        await Mock.WaitUntilAsync(
            () => chatIds.All(id => Mock.SentMessages.Any(m => m.ChatId == id && m.Text.Contains("one-on-one"))),
            "the refusals");

        Assert.All(Mock.SentMessages.Where(m => chatIds.Contains(m.ChatId)), m => Assert.Null(m.ParseMode));
        Assert.Empty(Router.Requests);
        Assert.DoesNotContain(Mock.SentMessages,
            m => chatIds.Contains(m.ChatId) && m.Text.Contains("Ask me anything"));

        // later messages in an already-refused chat stay silent; the fresh sentinel chat proves
        // the repeat was processed because the poller handles updates in order
        const long sentinelChatId = -523;
        Mock.EnqueueTextMessage(token, groupChatId, fromUserId: 520, "hello again", chatType: "group");
        Mock.EnqueueTextMessage(token, sentinelChatId, fromUserId: 520, "hello", chatType: "group");
        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == sentinelChatId && m.Text.Contains("one-on-one")),
            "the sentinel refusal");
        await Task.Delay(400);

        Assert.All(chatIds, id =>
            Assert.Single(Mock.SentMessages, m => m.ChatId == id && m.Text.Contains("one-on-one")));

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            Assert.NotNull(await session.LoadAsync<object>(conversationId));
            Assert.NotNull(await session.LoadAsync<object>(ConversationPreview.IdFor(conversationId)));
        }

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
            () => Mock.SentMessages.Any(m => m.ChatId == 400 && m.Text.Contains("fake agent")) ||
                  Mock.EditedMessages.Any(e => e.ChatId == 400 && e.Text.Contains("fake agent")),
            "the recovery reply");

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Polling_recovers_after_a_transient_getUpdates_failure()
    {
        var (app, channelId, token) = await ProvisionAsync();
        await using var appGuard = app;

        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(token) >= 1, "polling to start");

        Mock.GetUpdatesFailure = MockTelegramBotApi.Unauthorized;
        var failing = Mock.GetUpdatesCallCount(token);
        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(token) > failing, "a failing poll");

        Mock.GetUpdatesFailure = null;
        Mock.EnqueueTextMessage(token, chatId: 640, fromUserId: 640, "after recovery");
        await Mock.WaitUntilAsync(() => Router.Requests.Any(r => r.Prompt == "after recovery"), "the recovered run");

        await app.DeleteChannelAsync(channelId);
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
        Dictionary<string, ChannelParameterBinding>? bindings = null, AiAgentParameter[]? declared = null)
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
            ChannelType.Telegram, agentId, null, Telegram: new(token, bindings)));

        return (app, created.ChannelId, token);
    }
}
