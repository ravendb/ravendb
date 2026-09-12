using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
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
public class SlackWebhookTests(ITestOutputHelper output, QuillSlackFixture fixture)
    : QuillSlackTestBase(output, fixture)
{
    private const string Sender = "U0SENDER01";
    private const string DmChannel = "D0CHANNEL1";

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Url_verification_echoes_the_challenge_only_when_signed()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        var raw = JsonSerializer.SerializeToUtf8Bytes(new { type = "url_verification", challenge = "42" });

        var ok = await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));
        Assert.Equal(HttpStatusCode.OK, ok.StatusCode);
        Assert.Equal("42", await ok.Content.ReadAsStringAsync());
        Assert.StartsWith("text/plain", ok.Content.Headers.ContentType?.MediaType);

        var unsigned = await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, "not-the-secret"));
        Assert.Equal(HttpStatusCode.Unauthorized, unsigned.StatusCode);

        var unknown = await Host.Client.SendAsync(SignedPost(Guid.NewGuid().ToString("N"), raw, channel.SigningSecret));
        Assert.Equal(HttpStatusCode.OK, unknown.StatusCode);
        Assert.Empty(await unknown.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Signed_dm_runs_the_agent_and_streams_the_reply_through_an_edit()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app,
            new AiAgentParameter("slackUser", "the sender's Slack user id"));

        var raw = EventBytes(channel.TeamId, "Ev0001", DmMessage(Sender, "What are your hours?"));
        var response = await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

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

        var raw = EventBytes(channel.TeamId, "Ev0002", DmMessage(Sender, "count"));
        await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));

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

        var raw = EventBytes(channel.TeamId, "Ev0003", DmMessage(Sender, "long please"));
        await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));

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

        var raw = EventBytes(channel.TeamId, "Ev0004", DmMessage(Sender, "hi"));
        await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));

        await Slack.WaitUntilAsync(
            () => Slack.EditedMessages.Any(e => e.Text == "Hello from the fake agent."),
            "the finalized edit after the rate-limit retry");
        Assert.DoesNotContain(Slack.SentMessages, m => m.Text == SlackInboundProcessor.ErrorReply);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Redelivered_event_ids_dispatch_once()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        var raw = EventBytes(channel.TeamId, "Ev-same", DmMessage(Sender, "only once"));
        Assert.Equal(HttpStatusCode.OK,
            (await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret))).StatusCode);
        Assert.Equal(HttpStatusCode.OK,
            (await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret))).StatusCode);

        await Slack.WaitUntilAsync(() => Router.Requests.Count >= 1, "the first dispatch");
        await Task.Delay(250);
        Assert.Single(Router.Requests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delivery_with_a_bad_or_missing_signature_is_401_and_never_dispatches()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        var raw = EventBytes(channel.TeamId, "Ev0005", DmMessage(Sender, "hi"));

        var badSignature = await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, "not-the-secret"));
        Assert.Equal(HttpStatusCode.Unauthorized, badSignature.StatusCode);

        var unsigned = new HttpRequestMessage(HttpMethod.Post, QuillRoutes.SlackWebhook(channel.WebhookToken))
        {
            Content = JsonContent(raw),
        };
        var noSignature = await Host.Client.SendAsync(unsigned);
        Assert.Equal(HttpStatusCode.Unauthorized, noSignature.StatusCode);

        Assert.Empty(Router.Requests);
        Assert.Empty(Slack.SentMessages);

        var rows = await QuillHttp.GetAsync<SlackChannelHealthResponse[]>(
            Host.Client, QuillRoutes.SlackHealth(app.Slug));
        Assert.NotNull(Assert.Single(rows, r => r.ChannelId == channel.ChannelId).LastSignatureFailureAt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Stale_timestamps_are_rejected_as_replays()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        var raw = EventBytes(channel.TeamId, "Ev0006", DmMessage(Sender, "old"));
        var staleTimestamp = DateTimeOffset.UtcNow.AddMinutes(-10).ToUnixTimeSeconds();

        var response = await Host.Client.SendAsync(
            SignedPost(channel.WebhookToken, raw, channel.SigningSecret, staleTimestamp));

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Empty(Router.Requests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Deliveries_for_unknown_tokens_and_disabled_channels_drop_with_200()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        var unknown = await Host.Client.SendAsync(SignedPost(
            Guid.NewGuid().ToString("N"),
            EventBytes(channel.TeamId, "Ev0007", DmMessage(Sender, "hi")),
            channel.SigningSecret));
        Assert.Equal(HttpStatusCode.OK, unknown.StatusCode);

        await app.UpdateChannelAsync(channel.ChannelId, new UpdateChannelRequest(null, null, false));
        var disabled = await Host.Client.SendAsync(SignedPost(
            channel.WebhookToken,
            EventBytes(channel.TeamId, "Ev0008", DmMessage(Sender, "hi")),
            channel.SigningSecret));
        Assert.Equal(HttpStatusCode.OK, disabled.StatusCode);

        await Task.Delay(250);
        Assert.Empty(Router.Requests);
        Assert.Empty(Slack.SentMessages);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Bot_echoes_foreign_teams_and_non_dm_events_are_ignored()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        var deliveries = new[]
        {
            EventBytes(channel.TeamId, "Ev-bot", DmMessage(Sender, "echo", botId: "B0MOCK")),
            EventBytes(channel.TeamId, "Ev-sub", DmMessage(Sender, "edited", subtype: "message_changed")),
            EventBytes(channel.TeamId, "Ev-own", DmMessage(channel.BotUserId, "self")),
            EventBytes(channel.TeamId, "Ev-chn", DmMessage(Sender, "in a channel", channelType: "channel")),
            EventBytes("TOTHERTEAM", "Ev-team", DmMessage(Sender, "foreign")),
        };

        foreach (var raw in deliveries)
        {
            var response = await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        await Task.Delay(250);
        Assert.Empty(Router.Requests);
        Assert.Empty(Slack.SentMessages);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task File_shares_get_the_unsupported_kind_reply()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        var raw = EventBytes(channel.TeamId, "Ev0009", DmMessage(Sender, "see attached", subtype: "file_share"));
        var response = await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        await Slack.WaitUntilAsync(() => Slack.SentMessages.Count == 1, "the unsupported-kind reply");
        Assert.Equal(SlackInboundProcessor.UnsupportedKindReply, Slack.SentMessages[0].Text);
        Assert.Empty(Router.Requests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Oversized_deliveries_are_413()
    {
        await using var app = await NewAppAsync();
        var channel = await NewChannelAsync(app);

        var oversized = new byte[300_000];
        var response = await Host.Client.SendAsync(SignedPost(channel.WebhookToken, oversized, channel.SigningSecret));

        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, response.StatusCode);
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

        await Host.Client.SendAsync(SignedPost(channel.WebhookToken,
            EventBytes(channel.TeamId, "Ev-one", DmMessage(Sender, "one")), channel.SigningSecret));
        await Host.Client.SendAsync(SignedPost(channel.WebhookToken,
            EventBytes(channel.TeamId, "Ev-two", DmMessage(Sender, "two")), channel.SigningSecret));

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

        var raw = EventBytes(channel.TeamId, "Ev0010", DmMessage(Sender, "hi"));
        await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));

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

        var raw = EventBytes(channel.TeamId, "Ev0011", DmMessage(Sender, "format"));
        await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));

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

        var raw = EventBytes(channel.TeamId, "Ev-mail-1", DmMessage(Sender, "who am i?"));
        var response = await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

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
        {
            var raw = EventBytes(channel.TeamId, eventId, DmMessage(Sender, "again"));
            await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));
        }

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

        var raw = EventBytes(channel.TeamId, "Ev-mail-4", DmMessage(Sender, "who am i?"));
        await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));

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

        var raw = EventBytes(channel.TeamId, "Ev-mail-5", DmMessage(Sender, "who am i?"));
        await Host.Client.SendAsync(SignedPost(channel.WebhookToken, raw, channel.SigningSecret));

        await Slack.WaitUntilAsync(
            () => Slack.SentMessages.Any(m => m.Text == SlackInboundProcessor.ErrorReply), "the error reply");
        Assert.Empty(Router.Requests);
    }

    private sealed record ProvisionedChannel(
        string ChannelId, string BotToken, string SigningSecret, string WebhookToken,
        string TeamId, string BotUserId);

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
        var teamId = NewTeamId();
        var botUserId = NewBotUserId();
        var signingSecret = "signing-" + Guid.NewGuid().ToString("N");
        Slack.AddBot(botToken, teamId, "Webhook Test Co", botUserId);

        var bindings = parameter is null
            ? null
            : new Dictionary<string, ChannelParameterBinding>
            {
                [parameter.Name] = new() { Source = source },
            };

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Slack, agentId, null,
            Slack: new(botToken, signingSecret, ParameterBindings: bindings)));

        var info = await QuillHttp.GetAsync<SlackWebhookInfoResponse>(
            Host.Client, QuillRoutes.SlackWebhookInfo(app.Slug, created.ChannelId));
        var webhookToken = info.RequestUrl[(info.RequestUrl.LastIndexOf('/') + 1)..];

        return new ProvisionedChannel(created.ChannelId, botToken, signingSecret, webhookToken, teamId, botUserId);
    }

    private static object DmMessage(
        string user, string text, string? subtype = null, string? botId = null, string channelType = "im") => new
    {
        type = "message",
        subtype,
        channel = DmChannel,
        channel_type = channelType,
        user,
        bot_id = botId,
        text,
        ts = "1700000000.000100",
    };

    private static byte[] EventBytes(string teamId, string eventId, object message) =>
        JsonSerializer.SerializeToUtf8Bytes(new
        {
            type = "event_callback",
            team_id = teamId,
            event_id = eventId,
            @event = message,
        });

    private static HttpRequestMessage SignedPost(
        string webhookToken, byte[] raw, string signingSecret, long? timestamp = null)
    {
        var unixSeconds = timestamp ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var request = new HttpRequestMessage(HttpMethod.Post, QuillRoutes.SlackWebhook(webhookToken))
        {
            Content = JsonContent(raw),
        };

        var signedBytes = Encoding.ASCII.GetBytes($"v0:{unixSeconds}:").Concat(raw).ToArray();
        var mac = Convert.ToHexString(
            HMACSHA256.HashData(Encoding.UTF8.GetBytes(signingSecret), signedBytes)).ToLowerInvariant();
        request.Headers.Add("X-Slack-Signature", $"v0={mac}");
        request.Headers.Add("X-Slack-Request-Timestamp", unixSeconds.ToString());
        return request;
    }

    private static ByteArrayContent JsonContent(byte[] raw)
    {
        var content = new ByteArrayContent(raw);
        content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
        return content;
    }
}
