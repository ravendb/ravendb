using System.Net;
using System.Net.Http.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillTelegramCollection.Name)]
public class TelegramChannelEndpointsTests(ITestOutputHelper output, QuillTelegramFixture fixture)
    : QuillTelegramTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_requires_bot_token()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.Telegram, agentId, null)));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("botToken is required", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_an_unknown_agent_id()
    {
        await using var app = await NewAppAsync();

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, "no-such-agent", null, Telegram: new(NewBotToken()))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("unknown agentId 'no-such-agent'", e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_a_token_telegram_rejects_and_never_echoes_it()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        Mock.GetMeFailure = MockTelegramBotApi.Unauthorized;

        var token = NewBotToken();
        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(token))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("telegram rejected the bot token", e.Body);
        Assert.DoesNotContain(TokenSecret(token), e.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_and_rotation_reject_a_malformed_token()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, agentId, null, Telegram: new("not-a-valid-token"))));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("invalid bot token format", e.Body);

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(NewBotToken())));
        var rotate = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null, new("12noSecretPart"))));
        Assert.Equal(HttpStatusCode.BadRequest, rotate.StatusCode);
        Assert.Contains("invalid bot token format", rotate.Body);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_defaults_display_name_to_bot_username_and_projects_it()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(NewBotToken())));

        var channels = await app.GetChannelsAsync();
        var summary = Assert.Single(channels, c => c.ChannelId == created.ChannelId);
        Assert.Equal(ChannelType.Telegram, summary.Type);
        Assert.Equal("@quill_test_bot", summary.DisplayName);
        Assert.Equal("quill_test_bot", summary.Telegram?.BotUsername);
        Assert.True(summary.Enabled);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_a_second_channel_for_the_same_bot_in_one_app()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var token = NewBotToken();

        var first = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(token)));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(token))));

        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("already connected", e.Body);

        await app.DeleteChannelAsync(first.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_requires_a_binding_for_every_declared_parameter()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app,
            new AiAgentParameter("customerId", "the customer to scope queries to"),
            new AiAgentParameter("senderId", "the telegram user id"));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, agentId, null, Telegram: new(NewBotToken()))));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("missing parameter binding(s) for agent parameter(s): customerId, senderId", e.Body);

        var partial = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, agentId, null,
                Telegram: new(NewBotToken(), new Dictionary<string, TelegramParameterBinding>
                {
                    ["customerId"] = new() { Source = TelegramParameterSource.Constant, Value = "customers/1" },
                }))));
        Assert.Equal(HttpStatusCode.BadRequest, partial.StatusCode);
        Assert.Contains("missing parameter binding(s) for agent parameter(s): senderId", partial.Body);

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Telegram, agentId, null,
            Telegram: new(NewBotToken(), new Dictionary<string, TelegramParameterBinding>
            {
                ["customerId"] = new() { Source = TelegramParameterSource.Constant, Value = "customers/1" },
                ["senderId"] = new() { Source = TelegramParameterSource.UserId },
            })));
        Assert.NotEmpty(created.ChannelId);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_validates_the_parameter_bindings()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app,
            new AiAgentParameter("customerId", "the customer to scope queries to"));

        var undeclared = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, agentId, null,
                Telegram: new(NewBotToken(), new Dictionary<string, TelegramParameterBinding>
                {
                    ["customerId"] = new() { Source = TelegramParameterSource.Constant, Value = "customers/1" },
                    ["region"] = new() { Source = TelegramParameterSource.Constant, Value = "eu" },
                }))));
        Assert.Equal(HttpStatusCode.BadRequest, undeclared.StatusCode);
        Assert.Contains("undeclared agent parameter(s): region", undeclared.Body);

        var emptyConstant = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, agentId, null,
                Telegram: new(NewBotToken(), new Dictionary<string, TelegramParameterBinding>
                {
                    ["customerId"] = new() { Source = TelegramParameterSource.Constant },
                }))));
        Assert.Equal(HttpStatusCode.BadRequest, emptyConstant.StatusCode);
        Assert.Contains("a Constant binding requires a value", emptyConstant.Body);

        var valueOnWellKnown = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, agentId, null,
                Telegram: new(NewBotToken(), new Dictionary<string, TelegramParameterBinding>
                {
                    ["customerId"] = new() { Source = TelegramParameterSource.Username, Value = "alice" },
                }))));
        Assert.Equal(HttpStatusCode.BadRequest, valueOnWellKnown.StatusCode);
        Assert.Contains("a value applies only to Constant bindings", valueOnWellKnown.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_rejects_the_same_bot_in_another_app()
    {
        await using var app = await NewAppAsync();
        await using var other = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var otherAgentId = await SeedAgentAsync(other);
        var token = NewBotToken();

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(token)));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            other.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, otherAgentId, null, Telegram: new(token))));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("already connected in app", e.Body);
        Assert.Contains(app.Slug, e.Body);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_reclaims_an_orphaned_bot_reservation()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var token = NewBotToken();

        var first = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(token)));

        // deleting the channel doc out of band leaves the reservation orphaned
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            session.Delete(Channel.IdPrefix + first.ChannelId);
            await session.SaveChangesAsync();
        }

        var second = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(token)));
        Assert.NotEqual(first.ChannelId, second.ChannelId);

        await app.DeleteChannelAsync(second.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Rotation_reclaims_an_orphaned_bot_reservation()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var orphanToken = NewBotToken();

        var doomed = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(orphanToken)));
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            session.Delete(Channel.IdPrefix + doomed.ChannelId);
            await session.SaveChangesAsync();
        }

        var kept = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(NewBotToken())));
        var summary = await app.UpdateChannelAsync(kept.ChannelId,
            new UpdateChannelRequest(null, null, null, new(orphanToken)));
        Assert.Equal("quill_test_bot", summary.Telegram?.BotUsername);

        await app.DeleteChannelAsync(kept.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Parameter_bindings_are_replaceable_and_validated_after_provision()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app,
            new AiAgentParameter("customerId", "the customer to scope queries to"));

        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Telegram, agentId, null,
            Telegram: new(NewBotToken(), new Dictionary<string, TelegramParameterBinding>
            {
                ["customerId"] = new() { Source = TelegramParameterSource.Constant, Value = "customers/1" },
            })));

        var summary = await app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null,
            new(ParameterBindings: new Dictionary<string, TelegramParameterBinding>
            {
                ["customerId"] = new() { Source = TelegramParameterSource.Username },
            })));
        Assert.Equal(TelegramParameterSource.Username, summary.Telegram!.ParameterBindings["customerId"].Source);
        Assert.Null(summary.Telegram.ParameterBindings["customerId"].Value);

        var unchanged = await app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest("Renamed", null, null));
        Assert.Equal(TelegramParameterSource.Username, unchanged.Telegram!.ParameterBindings["customerId"].Source);

        var undeclared = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null,
                new(ParameterBindings: new Dictionary<string, TelegramParameterBinding>
                {
                    ["customerId"] = new() { Source = TelegramParameterSource.Username },
                    ["region"] = new() { Source = TelegramParameterSource.Constant, Value = "eu" },
                }))));
        Assert.Equal(HttpStatusCode.BadRequest, undeclared.StatusCode);
        Assert.Contains("undeclared agent parameter(s): region", undeclared.Body);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Binding_update_covers_an_agent_that_gained_a_parameter()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(NewBotToken())));

        await app.EditAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Telegram Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Parameters = [new AiAgentParameter("customerId", "the customer to scope queries to")],
        });

        var missing = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null,
                new(ParameterBindings: new Dictionary<string, TelegramParameterBinding>()))));
        Assert.Equal(HttpStatusCode.BadRequest, missing.StatusCode);
        Assert.Contains("missing parameter binding(s) for agent parameter(s): customerId", missing.Body);

        var summary = await app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null,
            new(ParameterBindings: new Dictionary<string, TelegramParameterBinding>
            {
                ["customerId"] = new() { Source = TelegramParameterSource.UserId },
            })));
        Assert.Equal(TelegramParameterSource.UserId, summary.Telegram!.ParameterBindings["customerId"].Source);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Message_overrides_are_validated_normalized_and_projected()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(NewBotToken())));

        var summary = await app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null,
            new(Messages: new TelegramChannelMessages { Greeting = "  Cześć!  ", UsernameMissing = "   " })));
        Assert.Equal("Cześć!", summary.Telegram?.Messages?.Greeting);
        Assert.Null(summary.Telegram?.Messages?.UsernameMissing);

        var listed = Assert.Single(await app.GetChannelsAsync(), c => c.ChannelId == created.ChannelId);
        Assert.Equal("Cześć!", listed.Telegram?.Messages?.Greeting);

        var unchanged = await app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest("Renamed", null, null));
        Assert.Equal("Cześć!", unchanged.Telegram?.Messages?.Greeting);

        var tooLong = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null,
                new(Messages: new TelegramChannelMessages { Greeting = new string('x', 4097) }))));
        Assert.Equal(HttpStatusCode.BadRequest, tooLong.StatusCode);
        Assert.Contains("messages.greeting exceeds", tooLong.Body);

        var controlChars = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null,
                new(Messages: new TelegramChannelMessages { NotConfigured = "beepbeep" }))));
        Assert.Equal(HttpStatusCode.BadRequest, controlChars.StatusCode);
        Assert.Contains("messages.notConfigured contains control characters", controlChars.Body);

        var cleared = await app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null,
            new(Messages: new TelegramChannelMessages())));
        Assert.Null(cleared.Telegram?.Messages);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Message_overrides_are_rejected_for_iframe_channels()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, agentId, []));

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null,
                new(Messages: new TelegramChannelMessages { Greeting = "Hello" }))));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("telegram settings apply to Telegram channels only", e.Body);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_and_list_never_contain_the_bot_token()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var token = NewBotToken();

        var resp = await Host.Client.PostAsJsonAsync(QuillRoutes.SetupChannel(app.Slug),
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(token)), QuillHttp.Json);
        var provisionBody = await resp.Content.ReadAsStringAsync();
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.DoesNotContain(TokenSecret(token), provisionBody);

        var listBody = await Host.Client.GetStringAsync(QuillRoutes.Channels(app.Slug));
        Assert.DoesNotContain(TokenSecret(token), listBody);

        var channels = await app.GetChannelsAsync();
        await app.DeleteChannelAsync(channels.Single().ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_and_update_reject_allowed_origins()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);

        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.ProvisionChannelAsync(new ProvisionChannelRequest(
                ChannelType.Telegram, agentId, new[] { "http://localhost" }, Telegram: new(NewBotToken()))));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Contains("allowedOrigins does not apply", e.Body);

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(NewBotToken())));
        var update = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, ["http://localhost"], null)));
        Assert.Equal(HttpStatusCode.BadRequest, update.StatusCode);
        Assert.Contains("allowedOrigins does not apply", update.Body);

        // an empty array is accepted on both arms, matching provision
        var emptyOrigins = await app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, [], null));
        Assert.Equal(ChannelType.Telegram, emptyOrigins.Type);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Token_rotation_revalidates_and_restarts_the_poller()
    {
        await using var app = await NewAppAsync();
        var agentId = await SeedAgentAsync(app);
        var original = NewBotToken();

        var created = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.Telegram, agentId, null, Telegram: new(original)));
        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(original) >= 1, "a poll with the original token");

        var rotated = NewBotToken();
        var summary = await app.UpdateChannelAsync(created.ChannelId,
            new UpdateChannelRequest(null, null, null, new(rotated)));
        Assert.Equal("quill_test_bot", summary.Telegram?.BotUsername);

        await Mock.WaitUntilAsync(() => Mock.GetUpdatesCallCount(rotated) >= 1, "a poll with the rotated token");

        Mock.GetMeFailure = MockTelegramBotApi.Unauthorized;
        var e = await Assert.ThrowsAsync<QuillHttpException>(() =>
            app.UpdateChannelAsync(created.ChannelId, new UpdateChannelRequest(null, null, null, Telegram: new(NewBotToken()))));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);

        await app.DeleteChannelAsync(created.ChannelId);
    }

    private static async Task<string> SeedAgentAsync(QuillApp app, params AiAgentParameter[] parameters)
    {
        var agentId = "tg-agent-" + Guid.NewGuid().ToString("N")[..8];
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Telegram Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Parameters = parameters.ToList(),
        });
        return agentId;
    }

    private static string TokenSecret(string token) => token[(token.IndexOf(':') + 1)..];
}
