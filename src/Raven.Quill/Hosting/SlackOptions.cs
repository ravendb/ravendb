namespace Raven.Quill.Hosting;

public sealed class SlackOptions
{
    public const int ApiMessageLimit = 40_000;

    public string ApiUrl { get; set; } = "https://slack.com/api";

    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(15);

    public int MessageLimit { get; set; } = 4000;

    public TimeSpan EditDebounce { get; set; } = TimeSpan.FromSeconds(2);

    public int SenderQueueCapacity { get; set; } = 8;

    public TimeSpan ApplyChangesInterval { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan SocketRestartDelay { get; set; } = TimeSpan.FromMinutes(5);
}
