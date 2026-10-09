using Microsoft.Extensions.Options;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Discord;

internal static class DiscordChannelRegistration
{
    public static IServiceCollection AddDiscordChannel(this IServiceCollection services)
    {
        services.AddOptions<ApplianceOptions>()
            .Configure(o => EnvironmentVariables.Read(Constants.Configuration.DiscordApiUrl, v => o.Discord.ApiUrl = v))
            .Validate(o => Uri.TryCreate(o.Discord.ApiUrl, UriKind.Absolute, out var u) &&
                           (u.Scheme == Uri.UriSchemeHttp || u.Scheme == Uri.UriSchemeHttps),
                "Discord ApiUrl must be an absolute http(s) URL")
            .Validate(o => o.Discord.RequestTimeout > TimeSpan.Zero, "Discord RequestTimeout must be positive")
            .Validate(o => o.Discord.MessageLimit is > 0 and <= DiscordOptions.ApiMessageLimit,
                $"Discord MessageLimit must be between 1 and {DiscordOptions.ApiMessageLimit}")
            .Validate(o => o.Discord.EditDebounce > TimeSpan.Zero, "Discord EditDebounce must be positive")
            .Validate(o => o.Discord.GatewayBackoffMax > TimeSpan.Zero, "Discord GatewayBackoffMax must be positive")
            .Validate(o => o.Discord.GatewayHandshakeTimeout > TimeSpan.Zero,
                "Discord GatewayHandshakeTimeout must be positive")
            .Validate(o => o.Discord.GatewayRestartDelay > TimeSpan.Zero, "Discord GatewayRestartDelay must be positive")
            .Validate(o => o.Discord.MaxGatewayFrameBytes > 0, "Discord MaxGatewayFrameBytes must be positive");

        services.AddHttpClient(DiscordApiClient.HttpClientName, static (sp, http) =>
        {
            var opts = sp.GetRequiredService<IOptions<ApplianceOptions>>().Value.Discord;
            http.BaseAddress = new Uri(opts.ApiUrl.EndsWith('/') ? opts.ApiUrl : opts.ApiUrl + "/");
            http.Timeout = opts.RequestTimeout;
        });
        services.AddTransient<IDiscordClient>(sp => DiscordApiClient.Create(sp.GetRequiredService<IHttpClientFactory>()));

        return services.AddChannelProvider<DiscordRuntimeFactory, DiscordTurns, DiscordMessage, DiscordBot>();
    }
}
