using Raven.Quill.Telegram;

namespace Raven.Quill.Hosting;

public sealed class TelegramOptions
{
    public string? ApiUrl { get; set; }

    public TimeSpan EditDebounce { get; set; } = TimeSpan.FromSeconds(1);

    internal const int ApiMessageLimit = 4096;

    public int MessageLimit { get; set; } = ApiMessageLimit;

    public TimeSpan PollBackoffMax { get; set; } = TimeSpan.FromMinutes(1);
}
