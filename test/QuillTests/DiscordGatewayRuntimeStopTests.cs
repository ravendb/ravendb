using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Discord;
using Raven.Quill.Hosting;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class DiscordGatewayRuntimeStopTests
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Stop_timeout_throws_and_the_late_exit_still_cleans_up()
    {
        var hang = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new HangingDiscordClient(hang.Task);
        await using var services = new ServiceCollection()
            .AddSingleton<IDiscordClient>(client)
            .BuildServiceProvider();

        var channel = new Channel
        {
            Id = Channel.IdPrefix + "d2-stop-test",
            Type = ChannelType.Discord,
            DisplayName = "d2",
            AgentId = "agent",
            AllowedOrigins = [],
            Enabled = true,
            CreatedAt = DateTime.UtcNow,
            Discord = new DiscordSettings { BotToken = "bot-token", BotUserId = "1", BotUsername = "d2" },
        };

        var options = new DiscordOptions { GatewayStopTimeout = TimeSpan.FromMilliseconds(250) };
        var runtime = DiscordGatewayRuntime.Start(
            "db", channel, channelChangeVector: null, processor: null!, new DiscordHealthRegistry(),
            services.GetRequiredService<IServiceScopeFactory>(), options, NullLogger.Instance);

        await client.Started.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await Assert.ThrowsAsync<TimeoutException>(() => runtime.StopAsync());
        Assert.Null(runtime.ExitedAt);

        hang.SetResult();

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (runtime.ExitedAt is null && DateTime.UtcNow < deadline)
            await Task.Delay(25);
        Assert.NotNull(runtime.ExitedAt);

        await runtime.StopAsync();
    }

    private sealed class HangingDiscordClient(Task hang) : IDiscordClient
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<string> GetGatewayUrlAsync(string botToken, CancellationToken ct)
        {
            Started.TrySetResult();
            await hang;
            throw new OperationCanceledException();
        }

        public Task<(DiscordBotIdentity? Identity, string? Error, bool DiscordResponded)> GetBotIdentityAsync(
            string botToken, CancellationToken ct) => throw new NotSupportedException();

        public Task<string> CreateMessageAsync(string botToken, string channelId, string content, CancellationToken ct) =>
            throw new NotSupportedException();

        public Task EditMessageAsync(
            string botToken, string channelId, string messageId, string content, CancellationToken ct) =>
            throw new NotSupportedException();
    }
}
