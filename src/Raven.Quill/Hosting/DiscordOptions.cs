namespace Raven.Quill.Hosting;

public sealed class DiscordOptions
{
    public string ApiUrl { get; set; } = "https://discord.com/api/v10";

    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(15);

    internal const int ApiMessageLimit = 2000;

    public int MessageLimit { get; set; } = ApiMessageLimit;

    public TimeSpan EditDebounce { get; set; } = TimeSpan.FromSeconds(2);

    public int SenderQueueCapacity { get; set; } = 8;

    public int MaxConcurrentTurns { get; set; } = 32;

    public TimeSpan ApplyChangesInterval { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan GatewayBackoffMax { get; set; } = TimeSpan.FromMinutes(1);

    public TimeSpan GatewayHandshakeTimeout { get; set; } = TimeSpan.FromSeconds(15);

    public TimeSpan GatewayRestartDelay { get; set; } = TimeSpan.FromMinutes(5);

    public TimeSpan GatewayStopTimeout { get; set; } = TimeSpan.FromSeconds(10);

    public int MaxGatewayFrameBytes { get; set; } = 1024 * 1024;
}
