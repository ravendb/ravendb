using FastTests;
using Microsoft.Extensions.DependencyInjection;
using Raven.Quill.Channels;
using Raven.Quill.Discord;
using Raven.Quill.Hosting;
using Raven.Quill.Logging;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class DiscordRuntimeStopTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Stop_waits_for_the_gateway_to_exit_and_then_cleans_up()
    {
        var hang = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new HangingHandler(hang.Task);
        var services = new ServiceCollection();
        services.AddHttpClient(DiscordApiClient.HttpClientName, http => http.BaseAddress = new Uri("http://discord.test/"))
            .ConfigurePrimaryHttpMessageHandler(() => handler);
        await using var provider = services.BuildServiceProvider();

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

        var runtime = DiscordRuntime.Start(
            "db", channel, channelChangeVector: null,
            new ChannelChats<DiscordMessage>(turns: null!, new ApplianceOptions(), new QuillLogger<DiscordRuntime>().RavenLogger),
            provider.GetRequiredService<IHttpClientFactory>(), new DiscordOptions(), new QuillLogger<DiscordRuntime>());

        await handler.Started.Task.WaitAsync(WaitTimeout);

        var stop = runtime.StopAsync();
        Assert.False(stop.IsCompleted);
        Assert.Null(runtime.ExitedAt);

        hang.SetResult();

        await stop.WaitAsync(WaitTimeout);
        Assert.NotNull(runtime.ExitedAt);
    }

    private sealed class HangingHandler(Task hang) : HttpMessageHandler
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Started.TrySetResult();
            await hang;
            throw new OperationCanceledException();
        }
    }
}
