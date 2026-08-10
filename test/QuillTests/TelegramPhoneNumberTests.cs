using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Telegram;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillTelegramCollection.Name)]
public class TelegramPhoneNumberTests(ITestOutputHelper output, QuillTelegramFixture fixture)
    : QuillTelegramTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task First_message_requests_the_contact_and_skips_the_agent()
    {
        var (app, channelId, token) = await ProvisionWithPhoneAsync();
        await using var appGuard = app;

        const long chatId = 700;
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 700, "hello");

        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text.Contains("phone number")),
            "the contact request");

        var request = Assert.Single(Mock.SentMessages, m => m.ChatId == chatId);
        Assert.Contains("request_contact", request.ReplyMarkup);
        Assert.Empty(Router.Requests);
        Assert.DoesNotContain(Mock.ChatActions, a => a.ChatId == chatId);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Shared_contact_is_stored_and_the_next_message_dispatches_with_the_phone_number()
    {
        var (app, channelId, token) = await ProvisionWithPhoneAsync();
        await using var appGuard = app;

        const long chatId = 710;
        Mock.EnqueueContactMessage(token, chatId, fromUserId: 710, "+48123456789");

        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text.Contains("send your message again")),
            "the confirmation");
        var confirmation = Assert.Single(Mock.SentMessages, m => m.ChatId == chatId);
        Assert.Contains("remove_keyboard", confirmation.ReplyMarkup);

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            var stored = await session.LoadAsync<TelegramUserPhone>(TelegramUserPhone.IdFor(channelId, 710));
            Assert.Equal("+48123456789", stored.PhoneNumber);
        }

        Mock.EnqueueTextMessage(token, chatId, fromUserId: 710, "hello");
        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 1, "the agent run");

        var request = Assert.Single(Router.Requests);
        Assert.Equal("+48123456789", request.Parameters["phone"]);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Someone_elses_contact_is_rejected_and_never_stored()
    {
        var (app, channelId, token) = await ProvisionWithPhoneAsync();
        await using var appGuard = app;

        const long chatId = 720;
        Mock.EnqueueContactMessage(token, chatId, fromUserId: 720, "+48111111111", contactUserId: 999);

        await Mock.WaitUntilAsync(
            () => Mock.SentMessages.Any(m => m.ChatId == chatId && m.Text.Contains("your own")),
            "the rejection");
        var rejection = Assert.Single(Mock.SentMessages, m => m.ChatId == chatId);
        Assert.Contains("request_contact", rejection.ReplyMarkup);

        using (var session = app.Store.OpenAsyncSession(app.Slug))
            Assert.Null(await session.LoadAsync<TelegramUserPhone>(TelegramUserPhone.IdFor(channelId, 720)));

        Assert.Empty(Router.Requests);

        await app.DeleteChannelAsync(channelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Contact_shares_are_ignored_when_no_parameter_maps_to_the_phone_number()
    {
        var app = await NewAppAsync();
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
            ChannelType.Telegram, agentId, null, BotToken: token));

        const long chatId = 730;
        Mock.EnqueueContactMessage(token, chatId, fromUserId: 730, "+48222222222");
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 730, "hello");

        await Mock.WaitUntilAsync(() => Router.Requests.Count >= 1, "the agent run");

        var request = Assert.Single(Router.Requests);
        Assert.Equal("hello", request.Prompt);
        Assert.DoesNotContain(Mock.SentMessages, m => m.ChatId == chatId && m.Text.Contains("phone number"));

        await app.DeleteChannelAsync(created.ChannelId);
    }

    private async Task<(QuillApp App, string ChannelId, string Token)> ProvisionWithPhoneAsync()
    {
        var app = await NewAppAsync();
        var agentId = "tg-agent-" + Guid.NewGuid().ToString("N")[..8];
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Telegram Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Parameters = [new AiAgentParameter("phone", "the sender's phone number")],
        });

        var token = NewBotToken();
        var created = await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Telegram, agentId, null, BotToken: token,
            ParameterBindings: new Dictionary<string, TelegramParameterBinding>
            {
                ["phone"] = new() { Source = TelegramParameterSource.PhoneNumber },
            }));

        return (app, created.ChannelId, token);
    }
}
