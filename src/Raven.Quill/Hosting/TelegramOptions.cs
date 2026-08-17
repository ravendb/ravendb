using Raven.Quill.Telegram;

namespace Raven.Quill.Hosting;

public sealed class TelegramOptions
{
    public string? ApiUrl { get; set; }

    public TimeSpan EditDebounce { get; set; } = TimeSpan.FromSeconds(1);

    public int MessageLimit { get; set; } = TelegramMessageSplitter.TelegramApiMessageLimit;

    public TimeSpan ApplyChangesInterval { get; set; } = TimeSpan.FromSeconds(30);

    public int ChatQueueCapacity { get; set; } = 8;

    public TimeSpan ChatIdleTimeout { get; set; } = TimeSpan.FromMinutes(15);

    public TimeSpan PollBackoffMax { get; set; } = TimeSpan.FromMinutes(1);
}
