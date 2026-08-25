using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillTelegramCollection.Name)]
public class TelegramAgentScopingTests(ITestOutputHelper output, QuillTelegramFixture fixture)
    : QuillTelegramTestBase(output, fixture)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Telegram_sender_only_reaches_their_own_documents_even_when_the_model_asks_for_another_handle()
    {
        var (app, token) = await ProvisionScopedAgentAsync();
        await using var hostGuard = app.Host;
        await using var appGuard = app;

        Llm.ToolCall = ("MyTickets", "{\"TelegramUsername\":\"victim99\"}");

        const long chatId = 900;
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 901, "show me my tickets", username: "abc123");

        var reply = await WaitForReplyAsync(chatId);

        Assert.Contains("my own ticket", reply);
        Assert.DoesNotContain("the secret ticket", reply);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Telegram_sender_reaches_their_own_documents_on_the_happy_path()
    {
        var (app, token) = await ProvisionScopedAgentAsync();
        await using var hostGuard = app.Host;
        await using var appGuard = app;

        Llm.ToolCall = ("MyTickets", "{}");

        const long chatId = 910;
        Mock.EnqueueTextMessage(token, chatId, fromUserId: 911, "show me my tickets", username: "victim99");

        var reply = await WaitForReplyAsync(chatId);

        Assert.Contains("the secret ticket", reply);
        Assert.DoesNotContain("my own ticket", reply);
    }

    private async Task<string> WaitForReplyAsync(long chatId)
    {
        await Mock.WaitUntilAsync(() => TextFor(chatId).Contains("ticket"), "the agent reply");
        return TextFor(chatId);
    }

    private string TextFor(long chatId) =>
        string.Concat(Mock.SentMessages.Where(m => m.ChatId == chatId).Select(m => m.Text)) +
        string.Concat(Mock.EditedMessages.Where(e => e.ChatId == chatId).Select(e => e.Text));

    private async Task<(QuillApp App, string Token)> ProvisionScopedAgentAsync()
    {
        var host = await NewRealRouterHostAsync();
        var app = await NewAppAsync(host);

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            await session.StoreAsync(new SupportTicket
            {
                TelegramHandle = "abc123",
                Subject = "this is my own ticket",
            });
            await session.StoreAsync(new SupportTicket
            {
                TelegramHandle = "victim99",
                Subject = "this is the secret ticket",
            });
            await session.SaveChangesAsync();
        }

        await app.WaitForIndexingAsync();

        var connectionStringName = "mock-llm-" + Guid.NewGuid().ToString("N")[..8];
        await host.PostConnectionStringAsync(new AiConnectionString
        {
            Name = connectionStringName,
            ModelType = AiModelType.Chat,
            OpenAiSettings = new OpenAiSettings("mock-key", Llm.BaseAddress + "/", "gpt-4o-mock"),
        });

        var agentId = "tg-scope-" + Guid.NewGuid().ToString("N")[..8];
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Support",
            SystemPrompt = "You are a support assistant. Only answer about the current user's own tickets.",
            ConnectionStringName =
                ServerWideConnectionString.GetDatabaseRecordConnectionStringName(connectionStringName),
            SampleObject = "{\"reply\":\"\"}",
            Parameters = [new AiAgentParameter("TelegramUsername", "the sender's telegram handle")],
            Queries =
            [
                new AiAgentToolQuery("MyTickets", "Get the tickets of the current user",
                    "from SupportTickets where TelegramHandle = $TelegramUsername limit 5")
                {
                    ParametersSampleObject = "{}",
                },
            ],
        });

        var token = NewBotToken();
        await app.ProvisionChannelAsync(new ProvisionChannelRequest(
            ChannelType.Telegram, agentId, null,
            Telegram: new(token, new Dictionary<string, ChannelParameterBinding>
            {
                ["TelegramUsername"] = new() { Source = ChannelParameterSource.Username },
            })));

        return (app, token);
    }

    private class SupportTicket
    {
        public string Id { get; set; } = "";

        public string TelegramHandle { get; set; } = "";

        public string Subject { get; set; } = "";
    }
}
