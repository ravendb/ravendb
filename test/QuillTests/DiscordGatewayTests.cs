using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Discord;
using Raven.Quill.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillDiscordCollection.Name)]
public class DiscordGatewayTests(ITestOutputHelper output, QuillDiscordFixture fixture)
    : QuillDiscordTestBase(output, fixture)
{
    private const string Sender = "800000000000000001";
    private const string DmChannel = "900000000000000001";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Identify_carries_the_dm_intent_and_the_bot_token()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        await Discord.WaitUntilConnectedAsync();

        var identify = Assert.Single(Discord.Identifies);
        Assert.Equal(channel.BotToken, identify.Token);
        Assert.Equal(MockDiscordApi.DirectMessagesIntent, identify.Intents & MockDiscordApi.DirectMessagesIntent);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Dm_runs_the_agent_and_streams_the_reply_through_an_edit()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app,
            new AiAgentParameter("discordUser", "the sender's Discord user id"));

        await Discord.DispatchDmAsync("msg-in-1", DmChannel, Sender, "What are your hours?");

        await Discord.WaitUntilAsync(() => Router.Requests.Count == 1, "the agent dispatch");
        var request = Assert.Single(Router.Requests);
        Assert.Equal("What are your hours?", request.Prompt);
        Assert.Equal(Channel.IdPrefix + channel.ChannelId, request.ChannelId);
        Assert.Matches($"^chats/discord/{channel.ChannelId}/{Sender}/\\d{{4}}-\\d{{2}}-\\d{{2}}$", request.ConversationId);
        Assert.Equal(Sender, request.Parameters["discordUser"].GetString());

        await Discord.WaitUntilAsync(
            () => Discord.EditedMessages.Any(e => e.Content == "Hello from the fake agent."), "the finalized edit");

        var sent = Assert.Single(Discord.SentMessages);
        Assert.Equal(DmChannel, sent.ChannelId);
        Assert.All(Discord.EditedMessages, e => Assert.Equal(sent.MessageId, e.MessageId));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Streaming_chunks_edit_the_same_message_in_place()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        Router.Chunks = ["One ", "two ", "three."];
        Router.ChunkDelay = TimeSpan.FromMilliseconds(120);

        await Discord.DispatchDmAsync("msg-in-2", DmChannel, Sender, "count");

        await Discord.WaitUntilAsync(
            () => Discord.EditedMessages.Any(e => e.Content == "One two three."), "the finalized edit");

        var sent = Assert.Single(Discord.SentMessages);
        Assert.All(Discord.EditedMessages, e => Assert.Equal(sent.MessageId, e.MessageId));
        Assert.True(Discord.EditedMessages.Count >= 2, "mid-stream previews must edit, not re-post");
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Long_replies_split_into_multiple_messages()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        var limit = new DiscordOptions().MessageLimit;
        var reply = new string('x', limit + 4);
        Router.Chunks = [reply];

        await Discord.DispatchDmAsync("msg-in-3", DmChannel, Sender, "long please");

        await Discord.WaitUntilAsync(() => Discord.SentMessages.Count == 2, "both reply parts");
        Assert.All(Discord.SentMessages, m => Assert.True(m.Content.Length <= limit));
        Assert.Equal(reply, string.Concat(Discord.SentMessages.Select(m => m.Content)));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Finalize_survives_one_rate_limited_edit()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        await Discord.WaitUntilConnectedAsync();
        Discord.NextEditRateLimit429 = true;
        Router.Chunks = ["Rate ", "limited ", "once."];
        Router.ChunkDelay = TimeSpan.FromMilliseconds(120);

        await Discord.DispatchDmAsync("msg-in-4", DmChannel, Sender, "retry");

        await Discord.WaitUntilAsync(
            () => Discord.EditedMessages.Any(e => e.Content == "Rate limited once."), "the finalized edit");
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Redelivered_message_ids_dispatch_once()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);

        await Discord.DispatchDmAsync("msg-dupe", DmChannel, Sender, "only once");
        await Discord.DispatchDmAsync("msg-dupe", DmChannel, Sender, "only once");

        await Discord.WaitUntilAsync(() => Router.Requests.Count == 1, "the single agent dispatch");
        await Task.Delay(250);
        Assert.Single(Router.Requests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Bot_authors_self_messages_and_guild_messages_are_ignored()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        await Discord.WaitUntilConnectedAsync();

        await Discord.DispatchDmAsync("msg-bot", DmChannel, "700000000000000009", "from a bot", fromBot: true);
        await Discord.DispatchDmAsync("msg-self", DmChannel, channel.BotUserId, "my own echo");
        await Discord.DispatchDmAsync("msg-guild", DmChannel, Sender, "in a server", guildId: "600000000000000001");

        await Task.Delay(400);
        Assert.Empty(Router.Requests);
        Assert.Empty(Discord.SentMessages);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Attachment_only_dms_get_the_unsupported_kind_reply()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);

        await Discord.DispatchDmAsync("msg-file", DmChannel, Sender, "", withAttachment: true);

        await Discord.WaitUntilAsync(
            () => Discord.SentMessages.Any(m => m.Content == DiscordInboundProcessor.UnsupportedKindReply),
            "the unsupported-kind reply");
        Assert.Empty(Router.Requests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Dms_that_carry_both_text_and_an_attachment_get_the_unsupported_kind_reply()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);

        await Discord.DispatchDmAsync(
            "msg-file-text", DmChannel, Sender, "here's the error I'm getting", withAttachment: true);

        await Discord.WaitUntilAsync(
            () => Discord.SentMessages.Any(m => m.Content == DiscordInboundProcessor.UnsupportedKindReply),
            "the unsupported-kind reply");
        Assert.Empty(Router.Requests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Sticker_dms_get_the_unsupported_kind_reply_and_land_in_health()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        await Discord.DispatchDmAsync("msg-sticker", DmChannel, Sender, "");

        await Discord.WaitUntilAsync(
            () => Discord.SentMessages.Any(m => m.Content == DiscordInboundProcessor.UnsupportedKindReply),
            "the unsupported-kind reply");
        Assert.Empty(Router.Requests);

        var health = await QuillHttp.GetAsync<DiscordChannelHealthResponse[]>(
            Host.Client, QuillRoutes.DiscordHealth(app.Slug));
        Assert.NotNull(health.Single(r => r.ChannelId == channel.ChannelId).LastInboundAt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Dm_system_messages_are_ignored()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        await Discord.WaitUntilConnectedAsync();

        await Discord.DispatchDmAsync("msg-call", DmChannel, Sender, "", messageType: 3);

        await Task.Delay(400);
        Assert.Empty(Router.Requests);
        Assert.Empty(Discord.SentMessages);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Replies_suppress_mentions_so_agent_text_cannot_ping_a_server()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        Router.Chunks = ["@everyone the deploy is done"];

        await Discord.DispatchDmAsync("msg-mention", DmChannel, Sender, "announce it");

        await Discord.WaitUntilAsync(() => Discord.SentMessages.Count > 0, "the reply");
        Assert.All(Discord.SentMessages, m =>
            Assert.True(m.MentionsSuppressed, "every send must carry allowed_mentions.parse = []"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Same_sender_turns_are_serialized()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        Router.Chunks = ["done"];
        Router.ChunkDelay = TimeSpan.FromMilliseconds(200);

        await Discord.DispatchDmAsync("msg-seq-1", DmChannel, Sender, "first");
        await Discord.DispatchDmAsync("msg-seq-2", DmChannel, Sender, "second");

        await Discord.WaitUntilAsync(() => Router.Requests.Count == 2, "both agent dispatches");
        Assert.Equal(["first", "second"], Router.Requests.Select(r => r.Prompt).ToArray());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Send_errors_surface_in_health_without_a_reply_loop()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        await Discord.WaitUntilConnectedAsync();
        Discord.SendErrorStatus = 403;

        await Discord.DispatchDmAsync("msg-send-error", DmChannel, Sender, "will fail");

        await Discord.WaitUntilAsync(
            async () =>
            {
                var rows = await QuillHttp.GetAsync<DiscordChannelHealthResponse[]>(
                    Host.Client, QuillRoutes.DiscordHealth(app.Slug));
                return rows.Single(r => r.ChannelId == channel.ChannelId).LastSendErrorAt is not null;
            },
            "the recorded send error");

        Assert.Empty(Discord.SentMessages);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Username_bound_parameters_resolve_from_the_message_author()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app,
            new AiAgentParameter("handle", "the sender's Discord username"), ChannelParameterSource.Username);

        await Discord.DispatchDmAsync("msg-handle", DmChannel, Sender, "who am i?", authorUsername: "dana.dev");

        await Discord.WaitUntilAsync(() => Router.Requests.Count == 1, "the agent dispatch");
        Assert.Equal("dana.dev", Assert.Single(Router.Requests).Parameters["handle"].GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Pausing_a_channel_disconnects_the_gateway()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        await Discord.WaitUntilConnectedAsync();

        await app.UpdateChannelAsync(channel.ChannelId, new UpdateChannelRequest(null, null, Enabled: false));

        await Discord.WaitUntilAsync(() => Discord.IsConnected == false, "the gateway to disconnect");
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_reconnect_request_resumes_with_the_stored_session()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        await Discord.WaitUntilConnectedAsync();

        var firstSessionId = Discord.CurrentSessionId;
        await Discord.RequestReconnectAsync();

        await Discord.WaitUntilAsync(() => Discord.Resumes.Count == 1, "the resume handshake");
        var resume = Assert.Single(Discord.Resumes);
        Assert.Equal(channel.BotToken, resume.Token);
        Assert.Equal(firstSessionId, resume.SessionId);
        Assert.True(resume.Seq >= 1, "the resume must carry the last sequence number the runtime saw");
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_timed_out_session_close_starts_a_new_session()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        await Discord.WaitUntilConnectedAsync();

        Discord.CloseAfterResume = 4009;
        await Discord.RequestReconnectAsync();

        await Discord.WaitUntilAsync(() => Discord.Resumes.Count == 1, "the rejected resume handshake");
        await Discord.WaitUntilAsync(() => Discord.Identifies.Count >= 2, "a fresh identify after the dead session");
        await Discord.WaitUntilConnectedAsync();
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_invalidated_resume_starts_a_new_session()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        await Discord.WaitUntilConnectedAsync();

        Discord.InvalidateResume = true;
        await Discord.RequestReconnectAsync();

        await Discord.WaitUntilAsync(() => Discord.Resumes.Count == 1, "the invalidated resume handshake");
        await Discord.WaitUntilAsync(() => Discord.Identifies.Count >= 2, "a fresh identify after the dead session");
        await Discord.WaitUntilConnectedAsync();
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_disallowed_intent_close_stops_reconnecting_and_surfaces_in_health()
    {
        await using var app = await NewAppAsync();
        Discord.CloseAfterIdentify = 4014;
        var channel = await NewChannelAsync(app);

        await Discord.WaitUntilAsync(
            async () =>
            {
                var rows = await QuillHttp.GetAsync<DiscordChannelHealthResponse[]>(
                    Host.Client, QuillRoutes.DiscordHealth(app.Slug));
                return rows.Single(r => r.ChannelId == channel.ChannelId).LastGatewayError is not null;
            },
            "the recorded gateway error");

        var health = await QuillHttp.GetAsync<DiscordChannelHealthResponse[]>(
            Host.Client, QuillRoutes.DiscordHealth(app.Slug));
        var row = health.Single(r => r.ChannelId == channel.ChannelId);
        Assert.False(row.GatewayConnected);
        Assert.Contains("direct messages intent", row.LastGatewayError);

        var identifiesAfterFatal = Discord.Identifies.Count;
        await Task.Delay(750);
        Assert.Equal(identifiesAfterFatal, Discord.Identifies.Count);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_invalid_token_close_stops_reconnecting_and_surfaces_in_health()
    {
        await using var app = await NewAppAsync();
        Discord.CloseAfterIdentify = 4004;
        var channel = await NewChannelAsync(app);

        await Discord.WaitUntilAsync(
            async () =>
            {
                var rows = await QuillHttp.GetAsync<DiscordChannelHealthResponse[]>(
                    Host.Client, QuillRoutes.DiscordHealth(app.Slug));
                return rows.Single(r => r.ChannelId == channel.ChannelId).LastGatewayError is not null;
            },
            "the recorded gateway error");

        var health = await QuillHttp.GetAsync<DiscordChannelHealthResponse[]>(
            Host.Client, QuillRoutes.DiscordHealth(app.Slug));
        Assert.Contains("rejected the bot token",
            health.Single(r => r.ChannelId == channel.ChannelId).LastGatewayError);

        var identifiesAfterFatal = Discord.Identifies.Count;
        await Task.Delay(750);
        Assert.Equal(identifiesAfterFatal, Discord.Identifies.Count);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_gateway_that_never_sends_hello_times_out_and_the_runtime_retries()
    {
        await using var app = await NewAppAsync();
        Discord.StallBeforeHello = true;
        var channel = await NewChannelAsync(app);

        await Discord.WaitUntilAsync(
            async () =>
            {
                var rows = await QuillHttp.GetAsync<DiscordChannelHealthResponse[]>(
                    Host.Client, QuillRoutes.DiscordHealth(app.Slug));
                return rows.Single(r => r.ChannelId == channel.ChannelId).LastGatewayError is not null;
            },
            "the recorded handshake timeout");

        var health = await QuillHttp.GetAsync<DiscordChannelHealthResponse[]>(
            Host.Client, QuillRoutes.DiscordHealth(app.Slug));
        Assert.Contains("hello frame", health.Single(r => r.ChannelId == channel.ChannelId).LastGatewayError);

        Discord.StallBeforeHello = false;
        await Discord.WaitUntilConnectedAsync();
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Repeated_failures_before_the_first_frame_drop_the_cached_session()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        await Discord.WaitUntilConnectedAsync();

        var connectsBefore = Discord.Connects;
        Discord.CloseOnConnect = 1000;
        await Discord.RequestReconnectAsync();

        await Discord.WaitUntilAsync(
            () => Discord.Connects >= connectsBefore + 4, "the attempts that never reach a frame");
        Discord.CloseOnConnect = null;

        await Discord.WaitUntilAsync(
            () => Discord.Identifies.Count >= 2, "a fresh identify once the cached session is dropped");
        await Discord.WaitUntilConnectedAsync();
        Assert.Empty(Discord.Resumes);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_fatally_stopped_gateway_reconnects_once_the_intent_is_enabled()
    {
        await using var app = await NewAppAsync();
        Discord.CloseAfterIdentify = 4014;
        var channel = await NewChannelAsync(app);

        await Discord.WaitUntilAsync(
            async () =>
            {
                var rows = await QuillHttp.GetAsync<DiscordChannelHealthResponse[]>(
                    Host.Client, QuillRoutes.DiscordHealth(app.Slug));
                return rows.Single(r => r.ChannelId == channel.ChannelId).LastGatewayError is not null;
            },
            "the recorded gateway error");

        Discord.CloseAfterIdentify = null;

        await Discord.WaitUntilConnectedAsync(TimeSpan.FromSeconds(30));
        Assert.True(Discord.Identifies.Count >= 2, "the stopped runtime must be restarted, not left dead");
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_rejected_bot_token_is_not_retried_after_the_restart_delay()
    {
        await using var app = await NewAppAsync();
        Discord.CloseAfterIdentify = 4004;
        var channel = await NewChannelAsync(app);

        await Discord.WaitUntilAsync(
            async () =>
            {
                var rows = await QuillHttp.GetAsync<DiscordChannelHealthResponse[]>(
                    Host.Client, QuillRoutes.DiscordHealth(app.Slug));
                return rows.Single(r => r.ChannelId == channel.ChannelId).LastGatewayError is not null;
            },
            "the recorded gateway error");

        Discord.CloseAfterIdentify = null;
        var identifiesAfterFatal = Discord.Identifies.Count;

        await Task.Delay(GatewayRestartDelay + TimeSpan.FromSeconds(2));

        Assert.Equal(identifiesAfterFatal, Discord.Identifies.Count);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_flooding_sender_is_capped_and_notified_once_per_burst()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        await Discord.WaitUntilConnectedAsync();

        var capacity = new DiscordOptions().SenderQueueCapacity;
        Router.Chunks = ["done"];

        var firstBurst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Router.BeforeRun = _ => firstBurst.Task;

        for (var i = 0; i < capacity + 2; i++)
            await Discord.DispatchDmAsync($"msg-flood-{i}", DmChannel, Sender, $"flood {i}");

        await Discord.WaitUntilAsync(() => OverloadNotices() == 1, "the single overload notice");
        Assert.Single(Router.Requests);

        firstBurst.SetResult();
        await Discord.WaitUntilAsync(() => Router.Requests.Count == capacity, "the capped burst to drain");
        Assert.Equal(1, OverloadNotices());

        var secondBurst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Router.BeforeRun = _ => secondBurst.Task;

        for (var i = 0; i < capacity + 2; i++)
            await Discord.DispatchDmAsync($"msg-flood2-{i}", DmChannel, Sender, $"again {i}");

        await Discord.WaitUntilAsync(() => OverloadNotices() == 2, "a fresh overload notice once the chain retired");

        secondBurst.SetResult();
        await Discord.WaitUntilAsync(
            () => Router.Requests.Count == capacity * 2, "the second capped burst to drain");
    }

    private int OverloadNotices() =>
        Discord.SentMessages.Count(m => m.Content == DiscordInboundProcessor.OverloadReply);

    private sealed record ProvisionedChannel(
        string ChannelId, string BotToken, string ApplicationId, string BotUserId);

    private async Task<ProvisionedChannel> NewChannelAsync(
        QuillApp app,
        AiAgentParameter? parameter = null,
        ChannelParameterSource source = ChannelParameterSource.UserId)
    {
        var agentId = "discord-agent-" + Guid.NewGuid().ToString("N")[..8];
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Discord Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Parameters = parameter is null ? [] : [parameter],
        });

        var botToken = NewBotToken();
        var applicationId = NewApplicationId();
        var botUserId = NewBotUserId();
        Discord.AddBot(botToken, applicationId, botUserId);

        var bindings = parameter is null
            ? null
            : new Dictionary<string, ChannelParameterBinding>
            {
                [parameter.Name] = new() { Source = source },
            };

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Discord, agentId, null,
            Discord: new(botToken, ParameterBindings: bindings)));

        return new ProvisionedChannel(created.ChannelId, botToken, applicationId, botUserId);
    }
}
