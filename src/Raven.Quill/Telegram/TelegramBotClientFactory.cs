using Microsoft.Extensions.Options;
using Raven.Quill.Hosting;
using Telegram.Bot;

namespace Raven.Quill.Telegram;

internal interface ITelegramBotClientFactory
{
    ITelegramBotClient Create(string botToken);
}

/// The single place the Bot API base URL is honored — tests point TelegramApiUrl at an in-process mock.
internal sealed class TelegramBotClientFactory(
    IOptions<ApplianceOptions> options,
    IHttpClientFactory httpClientFactory) : ITelegramBotClientFactory
{
    internal const string HttpClientName = "telegram";

    public ITelegramBotClient Create(string botToken)
    {
        var clientOptions = new TelegramBotClientOptions(botToken, baseUrl: options.Value.TelegramApiUrl);
        // named client: default 100s timeout comfortably above the 30s long-poll window
        return new TelegramBotClient(clientOptions, httpClientFactory.CreateClient(HttpClientName));
    }
}
