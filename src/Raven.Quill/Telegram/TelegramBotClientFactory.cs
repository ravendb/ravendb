using Microsoft.Extensions.Options;
using Raven.Quill.Hosting;
using Telegram.Bot;
using Telegram.Bot.Exceptions;
using TelegramUser = Telegram.Bot.Types.User;

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

internal static class TelegramBotTokenValidation
{
    public static async Task<(TelegramUser? Bot, string? Error)> ValidateBotTokenAsync(
        this ITelegramBotClientFactory botFactory, string botToken, CancellationToken ct)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(10));
            var bot = await botFactory.Create(botToken).GetMe(timeout.Token);
            return (bot, null);
        }
        catch (ArgumentException)
        {
            return (null, "invalid bot token format; expected '<botId>:<secret>' as issued by @BotFather");
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested == false)
        {
            return (null, "telegram did not respond while validating the bot token");
        }
        catch (ApiRequestException e)
        {
            return (null, $"telegram rejected the bot token: {e.Message}");
        }
        catch (RequestException e)
        {
            return (null, $"could not reach telegram: {e.Message}");
        }
    }
}
