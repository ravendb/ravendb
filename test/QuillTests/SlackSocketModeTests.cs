using System.Text.Json.Nodes;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Hosting;
using Raven.Quill.Slack;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillSlackCollection.Name)]
public class SlackSocketModeTests(ITestOutputHelper output, QuillSlackFixture fixture)
    : QuillSlackTestBase(output, fixture)
{
    private const string Sender = "U0SENDER01";
    private const string DmChannel = "D0CHANNEL1";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provisioning_opens_a_socket_with_the_app_token_and_health_reports_it_connected()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        await Slack.WaitUntilConnectedAsync();
        Assert.Contains(channel.AppToken, Slack.SocketOpenCalls);

        var rows = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
            Host.Client, QuillRoutes.SlackHealth(app.Slug));
        var row = Assert.Single(rows, r => r.ChannelId == channel.ChannelId);
        Assert.True(row.SocketConnected);
        Assert.NotNull(row.LastConnectedAt);
        Assert.Null(row.LastSocketError);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Dm_runs_the_agent_and_streams_the_reply_through_an_edit()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app,
            new AiAgentParameter("slackUser", "the sender's Slack user id"));

        await Slack.DispatchEventAsync(channel.TeamId, "Ev0001", DmMessage(Sender, "What are your hours?"));

        await Slack.WaitUntilAsync(() => Router.Requests.Count == 1, "the agent dispatch");
        var request = Assert.Single(Router.Requests);
        Assert.Equal("What are your hours?", request.Prompt);
        Assert.Equal(Channel.IdPrefix + channel.ChannelId, request.ChannelId);
        Assert.Matches($"^chats/slack/{channel.ChannelId}/{Sender}/\\d{{4}}-\\d{{2}}-\\d{{2}}$", request.ConversationId);
        Assert.Equal(Sender, request.Parameters["slackUser"].GetString());

        await Slack.WaitUntilAsync(
            () => Slack.EditedMessages.Any(e => e.Text == "Hello from the fake agent."), "the finalized edit");
        var sent = Assert.Single(Slack.SentMessages);
        Assert.Equal(DmChannel, sent.Channel);
        Assert.All(Slack.EditedMessages, e => Assert.Equal(sent.Ts, e.Ts));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Streaming_chunks_edit_the_same_message_in_place()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        Router.Chunks = ["One ", "two ", "three."];
        Router.ChunkDelay = TimeSpan.FromMilliseconds(120);

        await Slack.DispatchEventAsync(channel.TeamId, "Ev0002", DmMessage(Sender, "count"));

        await Slack.WaitUntilAsync(
            () => Slack.EditedMessages.Any(e => e.Text == "One two three."), "the finalized edit");

        var sent = Assert.Single(Slack.SentMessages);
        Assert.All(Slack.EditedMessages, e => Assert.Equal(sent.Ts, e.Ts));
        Assert.True(Slack.EditedMessages.Count >= 2, "mid-stream previews must edit, not re-post");
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Long_replies_split_into_multiple_messages()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        var limit = new SlackOptions().MessageLimit;
        var reply = new string('x', limit + 4);
        Router.Chunks = [reply];

        await Slack.DispatchEventAsync(channel.TeamId, "Ev0003", DmMessage(Sender, "long please"));

        await Slack.WaitUntilAsync(() => Slack.SentMessages.Count == 2, "both reply parts");
        Assert.All(Slack.SentMessages, m => Assert.True(m.Text.Length <= limit));
        Assert.Equal(reply, string.Concat(Slack.SentMessages.Select(m => m.Text)));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Finalize_survives_one_rate_limited_edit()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        Slack.NextUpdateRateLimit429 = true;

        await Slack.DispatchEventAsync(channel.TeamId, "Ev0004", DmMessage(Sender, "hi"));

        await Slack.WaitUntilAsync(
            () => Slack.EditedMessages.Any(e => e.Text == "Hello from the fake agent."),
            "the finalized edit after the rate-limit retry");
        Assert.DoesNotContain(Slack.SentMessages, m => m.Text == SlackInboundProcessor.ErrorReply);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Every_envelope_is_acked_including_the_ones_the_runtime_ignores()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        var dm = await Slack.DispatchEventAsync(channel.TeamId, "Ev-ack-1", DmMessage(Sender, "hi"));
        var interactive = await Slack.DispatchEnvelopeAsync(new JsonObject
        {
            ["type"] = "interactive",
            ["accepts_response_payload"] = true,
            ["payload"] = new JsonObject { ["type"] = "block_actions" },
        });
        var foreign = await Slack.DispatchEventAsync("TOTHERTEAM", "Ev-ack-2", DmMessage(Sender, "foreign"));

        await Slack.WaitUntilAsync(() => Slack.Acks.Count == 3, "all three acks");
        Assert.Equal(new[] { dm, interactive, foreign }, Slack.Acks);

        await Slack.WaitUntilAsync(() => Router.Requests.Count == 1, "the single agent dispatch");
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Redelivered_event_ids_dispatch_once()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        await Slack.DispatchEventAsync(channel.TeamId, "Ev-same", DmMessage(Sender, "only once"));
        await Slack.DispatchEventAsync(channel.TeamId, "Ev-same", DmMessage(Sender, "only once"));

        await Slack.WaitUntilAsync(() => Router.Requests.Count >= 1, "the first dispatch");
        await Task.Delay(250);
        Assert.Single(Router.Requests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Bot_echoes_foreign_teams_and_non_dm_events_are_ignored()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        await Slack.DispatchEventAsync(channel.TeamId, "Ev-bot", DmMessage(Sender, "echo", botId: "B0MOCK"));
        await Slack.DispatchEventAsync(channel.TeamId, "Ev-sub", DmMessage(Sender, "edited", subtype: "message_changed"));
        await Slack.DispatchEventAsync(channel.TeamId, "Ev-own", DmMessage(channel.BotUserId, "self"));
        await Slack.DispatchEventAsync(channel.TeamId, "Ev-chn", DmMessage(Sender, "in a channel", channelType: "channel"));
        await Slack.DispatchEventAsync("TOTHERTEAM", "Ev-team", DmMessage(Sender, "foreign"));

        await Slack.WaitUntilAsync(() => Slack.Acks.Count == 5, "all acks");
        await Task.Delay(250);
        Assert.Empty(Router.Requests);
        Assert.Empty(Slack.SentMessages);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task File_shares_get_the_unsupported_kind_reply()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        await Slack.DispatchEventAsync(channel.TeamId, "Ev0009", DmMessage(Sender, "see attached", subtype: "file_share"));

        await Slack.WaitUntilAsync(() => Slack.SentMessages.Count == 1, "the unsupported-kind reply");
        Assert.Equal(SlackInboundProcessor.UnsupportedKindReply, Slack.SentMessages[0].Text);
        Assert.Empty(Router.Requests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Same_sender_turns_are_serialized()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = 1;
        Router.BeforeRun = _ =>
            Interlocked.Exchange(ref first, 0) == 1 ? gate.Task : Task.CompletedTask;

        await Slack.DispatchEventAsync(channel.TeamId, "Ev-one", DmMessage(Sender, "one"));
        await Slack.DispatchEventAsync(channel.TeamId, "Ev-two", DmMessage(Sender, "two"));

        await Slack.WaitUntilAsync(() => Router.Requests.Count == 1, "the first dispatch");
        await Task.Delay(250);
        Assert.Single(Router.Requests);

        gate.SetResult();
        await Slack.WaitUntilAsync(() => Router.Requests.Count == 2, "the queued second dispatch");
        Assert.Equal("one", Router.Requests[0].Prompt);
        Assert.Equal("two", Router.Requests[1].Prompt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Send_errors_surface_in_health_without_a_reply_loop()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        Slack.SendError = "channel_not_found";

        await Slack.DispatchEventAsync(channel.TeamId, "Ev0010", DmMessage(Sender, "hi"));

        await Slack.WaitUntilAsync(async () =>
        {
            var rows = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
                Host.Client, QuillRoutes.SlackHealth(app.Slug));
            return rows.Single(r => r.ChannelId == channel.ChannelId).LastSendErrorAt is not null;
        }, "the send error to surface in health");

        await Task.Delay(250);
        Assert.Empty(Slack.SentMessages);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Markdown_replies_are_converted_to_mrkdwn_on_finalize()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        Router.Chunks = ["**Bold** and [docs](https://example.org/a)"];

        await Slack.DispatchEventAsync(channel.TeamId, "Ev0011", DmMessage(Sender, "format"));

        await Slack.WaitUntilAsync(
            () => Slack.EditedMessages.Any(e => e.Text == "*Bold* and <https://example.org/a|docs>"),
            "the mrkdwn-converted finalized edit");

        Assert.All(Slack.SentMessages, m => Assert.Equal("none", m.Parse));
        Assert.All(Slack.EditedMessages, e => Assert.Equal("none", e.Parse));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Email_bound_parameters_resolve_from_the_senders_slack_profile()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app,
            new AiAgentParameter("senderEmail", "the sender's email"), ChannelParameterSource.Email);
        Slack.AddUser(Sender, "dana@acme.example");

        await Slack.DispatchEventAsync(channel.TeamId, "Ev-mail-1", DmMessage(Sender, "who am i?"));

        await Slack.WaitUntilAsync(() => Router.Requests.Count == 1, "the agent dispatch");
        Assert.Equal("dana@acme.example", Assert.Single(Router.Requests).Parameters["senderEmail"].GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Repeat_senders_reuse_one_cached_profile_lookup()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app,
            new AiAgentParameter("senderEmail", "the sender's email"), ChannelParameterSource.Email);
        Slack.AddUser(Sender, "dana@acme.example");

        foreach (var eventId in new[] { "Ev-mail-2", "Ev-mail-3" })
            await Slack.DispatchEventAsync(channel.TeamId, eventId, DmMessage(Sender, "again"));

        await Slack.WaitUntilAsync(() => Router.Requests.Count == 2, "both agent dispatches");
        Assert.Equal(Sender, Assert.Single(Slack.UserInfoCalls));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_sender_without_an_email_gets_the_error_reply_and_never_dispatches()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app,
            new AiAgentParameter("senderEmail", "the sender's email"), ChannelParameterSource.Email);
        Slack.AddUser(Sender, email: null);

        await Slack.DispatchEventAsync(channel.TeamId, "Ev-mail-4", DmMessage(Sender, "who am i?"));

        await Slack.WaitUntilAsync(
            () => Slack.SentMessages.Any(m => m.Text == SlackInboundProcessor.ErrorReply), "the error reply");
        Assert.Empty(Router.Requests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_app_missing_the_users_read_scope_gets_the_error_reply_and_never_dispatches()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app,
            new AiAgentParameter("senderEmail", "the sender's email"), ChannelParameterSource.Email);
        Slack.AddUser(Sender, "dana@acme.example");
        Slack.UsersReadScopeGranted = false;

        await Slack.DispatchEventAsync(channel.TeamId, "Ev-mail-5", DmMessage(Sender, "who am i?"));

        await Slack.WaitUntilAsync(
            () => Slack.SentMessages.Any(m => m.Text == SlackInboundProcessor.ErrorReply), "the error reply");
        Assert.Empty(Router.Requests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Pausing_a_channel_disconnects_the_socket()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        await Slack.WaitUntilConnectedAsync();

        await app.UpdateChannelAsync(channel.ChannelId, new UpdateChannelRequest(null, null, Enabled: false));

        await Slack.WaitUntilAsync(() => Slack.IsConnected == false, "the socket to disconnect");
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_disconnect_frame_reconnects_through_a_fresh_socket_url()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        await Slack.WaitUntilConnectedAsync();

        var opensBefore = Slack.SocketOpenCalls.Count;
        var connectsBefore = Slack.Connects;
        await Slack.SendDisconnectAsync("refresh_requested");

        await Slack.WaitUntilAsync(() => Slack.Connects > connectsBefore, "a second socket connection");
        await Slack.WaitUntilConnectedAsync();
        Assert.True(Slack.SocketOpenCalls.Count > opensBefore, "the reconnect must ask Slack for a new socket url");
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_link_disabled_disconnect_stops_reconnecting_and_surfaces_in_health()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        await Slack.WaitUntilConnectedAsync();

        await Slack.SendDisconnectAsync("link_disabled");

        await Slack.WaitUntilAsync(
            async () =>
            {
                var rows = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
                    Host.Client, QuillRoutes.SlackHealth(app.Slug));
                return rows.Single(r => r.ChannelId == channel.ChannelId).LastSocketError is not null;
            },
            "the recorded socket error");

        var health = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
            Host.Client, QuillRoutes.SlackHealth(app.Slug));
        var row = health.Single(r => r.ChannelId == channel.ChannelId);
        Assert.False(row.SocketConnected);
        Assert.Contains("Socket Mode", row.LastSocketError);

        var opensAfterFatal = Slack.SocketOpenCalls.Count;
        await Task.Delay(750);
        Assert.Equal(opensAfterFatal, Slack.SocketOpenCalls.Count);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_revoked_app_token_stops_reconnecting_and_surfaces_in_health()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        await Slack.WaitUntilConnectedAsync();

        Slack.RemoveAppToken(channel.AppToken);
        await Slack.CloseCurrentAsync();

        await Slack.WaitUntilAsync(
            async () =>
            {
                var rows = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
                    Host.Client, QuillRoutes.SlackHealth(app.Slug));
                return rows.Single(r => r.ChannelId == channel.ChannelId).LastSocketError is not null;
            },
            "the recorded socket error");

        var health = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
            Host.Client, QuillRoutes.SlackHealth(app.Slug));
        Assert.Contains("rejected the app-level token",
            health.Single(r => r.ChannelId == channel.ChannelId).LastSocketError);

        var opensAfterFatal = Slack.SocketOpenCalls.Count;
        await Task.Delay(750);
        Assert.Equal(opensAfterFatal, Slack.SocketOpenCalls.Count);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_socket_that_never_sends_hello_times_out_and_the_runtime_retries()
    {
        await using var app = await NewAppAsync();
        Slack.StallBeforeHello = true;
        var channel = await NewChannelAsync(app);

        await Slack.WaitUntilAsync(
            async () =>
            {
                var rows = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
                    Host.Client, QuillRoutes.SlackHealth(app.Slug));
                return rows.Single(r => r.ChannelId == channel.ChannelId).LastSocketError is not null;
            },
            "the recorded handshake timeout");

        var health = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
            Host.Client, QuillRoutes.SlackHealth(app.Slug));
        Assert.Contains("hello frame", health.Single(r => r.ChannelId == channel.ChannelId).LastSocketError);

        Slack.StallBeforeHello = false;
        await Slack.WaitUntilConnectedAsync();
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_socket_closed_before_hello_surfaces_in_health_and_the_runtime_retries()
    {
        await using var app = await NewAppAsync();
        Slack.CloseOnConnect = true;
        var channel = await NewChannelAsync(app);

        await Slack.WaitUntilAsync(
            async () =>
            {
                var rows = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
                    Host.Client, QuillRoutes.SlackHealth(app.Slug));
                return rows.Single(r => r.ChannelId == channel.ChannelId).LastSocketError is not null;
            },
            "the recorded pre-hello close");

        var health = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
            Host.Client, QuillRoutes.SlackHealth(app.Slug));
        Assert.Contains("before sending a hello frame", health.Single(r => r.ChannelId == channel.ChannelId).LastSocketError);

        Slack.CloseOnConnect = false;
        await Slack.WaitUntilConnectedAsync();
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_transient_open_failure_retries_until_slack_recovers()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        await Slack.WaitUntilConnectedAsync();

        Slack.SocketOpenError = "internal_error";
        var opensBefore = Slack.SocketOpenCalls.Count;
        await Slack.CloseCurrentAsync();

        await Slack.WaitUntilAsync(() => Slack.SocketOpenCalls.Count >= opensBefore + 2, "repeated open attempts");
        Slack.SocketOpenError = null;

        await Slack.WaitUntilConnectedAsync();
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_rate_limited_open_waits_out_retry_after_before_trying_again()
    {
        await using var app = await NewAppAsync();
        await NewChannelAsync(app);
        await Slack.WaitUntilConnectedAsync();

        Slack.SocketOpenError = "ratelimited";
        Slack.SocketOpenRetryAfter = TimeSpan.FromSeconds(2);
        var opensBefore = Slack.SocketOpenCalls.Count;
        await Slack.CloseCurrentAsync();

        await Slack.WaitUntilAsync(() => Slack.SocketOpenCalls.Count == opensBefore + 1, "the first rate-limited open");
        await Task.Delay(1000);
        Assert.Equal(opensBefore + 1, Slack.SocketOpenCalls.Count);

        Slack.SocketOpenError = null;
        Slack.SocketOpenRetryAfter = null;
        await Slack.WaitUntilConnectedAsync();
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channels_without_an_app_token_stay_offline_and_health_says_why()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);
        await Slack.WaitUntilConnectedAsync();

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            var doc = await session.LoadAsync<Channel>(Channel.IdPrefix + channel.ChannelId);
            doc.Slack!.AppToken = "";
            await session.SaveChangesAsync();
        }

        await Slack.WaitUntilAsync(() => Slack.IsConnected == false, "the socket to disconnect");

        var rows = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
            Host.Client, QuillRoutes.SlackHealth(app.Slug));
        var row = Assert.Single(rows, r => r.ChannelId == channel.ChannelId);
        Assert.False(row.SocketConnected);
        Assert.Contains("predates Socket Mode", row.LastSocketError);
    }

    private sealed record ProvisionedChannel(
        string ChannelId, string BotToken, string AppToken, string TeamId, string BotUserId);

    private async Task<ProvisionedChannel> NewChannelAsync(
        QuillApp app,
        AiAgentParameter? parameter = null,
        ChannelParameterSource source = ChannelParameterSource.UserId)
    {
        var agentId = "slack-agent-" + Guid.NewGuid().ToString("N")[..8];
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Slack Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Parameters = parameter is null ? [] : [parameter],
        });

        var botToken = NewBotToken();
        var appToken = NewAppToken();
        var teamId = NewTeamId();
        var botUserId = NewBotUserId();
        Slack.AddBot(botToken, teamId, "Socket Test Co", botUserId);

        var bindings = parameter is null
            ? null
            : new Dictionary<string, ChannelParameterBinding>
            {
                [parameter.Name] = new() { Source = source },
            };

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null,
            DisplayName: "Support bot", Slack: new(botToken, appToken, ParameterBindings: bindings)));

        return new ProvisionedChannel(created.ChannelId, botToken, appToken, teamId, botUserId);
    }

    private static JsonObject DmMessage(
        string user, string text, string? subtype = null, string? botId = null, string channelType = "im")
    {
        var message = new JsonObject
        {
            ["type"] = "message",
            ["channel"] = DmChannel,
            ["channel_type"] = channelType,
            ["user"] = user,
            ["text"] = text,
            ["ts"] = "1700000000.000100",
        };
        if (subtype is not null)
            message["subtype"] = subtype;
        if (botId is not null)
            message["bot_id"] = botId;
        return message;
    }
}
