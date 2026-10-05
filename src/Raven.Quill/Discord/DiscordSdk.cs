using Discord;
using Discord.Net.Rest;
using Discord.Rest;
using Discord.WebSocket;
using Microsoft.Extensions.Options;
using Raven.Quill.Hosting;

namespace Raven.Quill.Discord;

internal sealed class DiscordSdk(IOptions<ApplianceOptions> options)
{
    private readonly DiscordOptions _options = options.Value.Discord;

    public DiscordRestClient NewRestClient() => new(Configure(new DiscordRestConfig()));

    public DiscordSocketClient NewSocketClient() => new(Configure(new DiscordSocketConfig
    {
        GatewayIntents = GatewayIntents.DirectMessages,
        ConnectionTimeout = (int)_options.GatewayHandshakeTimeout.TotalMilliseconds,
        LogGatewayIntentWarnings = false,
    }));

    public RequestOptions NewRequestOptions(CancellationToken ct) => new()
    {
        CancelToken = ct,
        Timeout = (int)_options.RequestTimeout.TotalMilliseconds,
    };

    private TConfig Configure<TConfig>(TConfig config) where TConfig : DiscordRestConfig
    {
        var apiUrl = _options.ApiUrl.TrimEnd('/') + "/";
        config.RestClientProvider = _ => DefaultRestClientProvider.Instance(apiUrl);
        config.DefaultRetryMode = RetryMode.RetryRatelimit | RetryMode.Retry502;
        config.LogLevel = LogSeverity.Info;
        return config;
    }
}
