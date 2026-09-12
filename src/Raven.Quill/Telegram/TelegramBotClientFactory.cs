using Microsoft.Extensions.Options;
using Raven.Quill.Hosting;
using Telegram.Bot;

namespace Raven.Quill.Telegram;

internal interface ITelegramBotClientFactory
{
    ITelegramBotClient Create(string botToken);
}

internal sealed class TelegramBotClientFactory(
    IOptions<ApplianceOptions> options,
    IHttpClientFactory httpClientFactory) : ITelegramBotClientFactory
{
    internal const string HttpClientName = "telegram";

    public ITelegramBotClient Create(string botToken)
    {
        var clientOptions = new TelegramBotClientOptions(botToken, baseUrl: options.Value.Telegram.ApiUrl);
        return new TelegramBotClient(clientOptions, httpClientFactory.CreateClient(HttpClientName));
    }
}
