using System.Diagnostics;
using FastTests;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Channels;
using Raven.Quill.Discord;
using Raven.Quill.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class DiscordInboundTurnGateTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Turns_across_senders_respect_the_global_concurrency_cap()
    {
        using var store = GetDocumentStore();
        var channelId = Channel.IdPrefix + "gate-test";
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Channel
            {
                Id = channelId,
                Type = ChannelType.Discord,
                DisplayName = "gate",
                AgentId = "agent",
                AllowedOrigins = [],
                Enabled = true,
                CreatedAt = DateTime.UtcNow,
                Discord = new DiscordSettings { BotToken = "token", BotUserId = "1", BotUsername = "gate" },
            });
            await session.SaveChangesAsync();
        }

        var client = new BlockingDiscordClient();
        await using var services = new ServiceCollection()
            .AddSingleton<IDiscordClient>(client)
            .BuildServiceProvider();

        var options = Microsoft.Extensions.Options.Options.Create(
            new ApplianceOptions { Discord = { MaxConcurrentTurns = 1 } });
        var processor = new DiscordInboundProcessor(
            store, new FakeAgentRouter(), services.GetRequiredService<IServiceScopeFactory>(),
            new DiscordHealthRegistry(), options, NullLogger<DiscordInboundProcessor>.Instance);

        processor.Enqueue(store.Database, channelId, "sender-1", null, "dm-1", "m1", "image", null);
        await client.WaitForCallsAsync(1);

        processor.Enqueue(store.Database, channelId, "sender-2", null, "dm-2", "m2", "image", null);
        await Task.Delay(300);
        Assert.Equal(1, client.Calls);

        client.Release();
        await client.WaitForCallsAsync(2);
        client.Release();

        await processor.StopAsync(CancellationToken.None);
    }

    private sealed class BlockingDiscordClient : IDiscordClient
    {
        private readonly SemaphoreSlim _gate = new(0);
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public void Release() => _gate.Release();

        public async Task WaitForCallsAsync(int count)
        {
            var sw = Stopwatch.StartNew();
            while (Calls < count)
            {
                if (sw.Elapsed > TimeSpan.FromSeconds(30))
                    throw new TimeoutException($"expected {count} discord calls, saw {Calls}");
                await Task.Delay(25);
            }
        }

        public async Task<string> CreateMessageAsync(string botToken, string channelId, string content, CancellationToken ct)
        {
            Interlocked.Increment(ref _calls);
            await _gate.WaitAsync(ct);
            return "message-1";
        }

        public Task EditMessageAsync(
            string botToken, string channelId, string messageId, string content, CancellationToken ct) =>
            Task.CompletedTask;

        public Task<(DiscordBotIdentity? Identity, string? Error, bool DiscordResponded)> GetBotIdentityAsync(
            string botToken, CancellationToken ct) => throw new NotSupportedException();

        public Task<string> GetGatewayUrlAsync(string botToken, CancellationToken ct) =>
            throw new NotSupportedException();
    }
}
