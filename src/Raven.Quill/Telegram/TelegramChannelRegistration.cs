using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Telegram;

internal static class TelegramChannelRegistration
{
    public static IServiceCollection AddTelegramChannel(this IServiceCollection services)
    {
        services.AddOptions<ApplianceOptions>()
            .Configure(o =>
                EnvironmentVariables.Read(Constants.Configuration.TelegramApiUrl, v => o.Telegram.ApiUrl = v))
            .Validate(o => string.IsNullOrEmpty(o.Telegram.ApiUrl) ||
                           Uri.TryCreate(o.Telegram.ApiUrl, UriKind.Absolute, out var u) &&
                           (u.Scheme == Uri.UriSchemeHttp || u.Scheme == Uri.UriSchemeHttps),
                "Telegram ApiUrl must be an absolute http(s) URL")
            .Validate(o => o.Telegram.MessageLimit is > 0 and <= TelegramOptions.ApiMessageLimit,
                $"Telegram MessageLimit must be between 1 and {TelegramOptions.ApiMessageLimit}")
            .Validate(o => o.Telegram.EditDebounce > TimeSpan.Zero, "Telegram EditDebounce must be positive")
            .Validate(o => o.Telegram.PollBackoffMax > TimeSpan.Zero, "Telegram PollBackoffMax must be positive");

        services.AddHttpClient(TelegramBotClientFactory.HttpClientName);
        services.AddSingleton<TelegramBotClientFactory>();

        return services.AddChannelProvider<TelegramRuntimeFactory, TelegramTurns, TelegramMessage, TelegramBot>();
    }
}
