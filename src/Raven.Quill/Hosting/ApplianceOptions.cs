using System.ComponentModel.DataAnnotations;

namespace Raven.Quill.Hosting;

public sealed class ApplianceOptions
{
    [Required]
    public string RavenUrl { get; set; } = "http://127.0.0.1:8080";

    [Required]
    public string WebListenUrl { get; set; } = "http://127.0.0.1:5000";

    [Required]
    public string ConfigDatabase { get; set; } = ApplianceDatabases.Config;

    public int RavenInternalPort { get; set; } = 8443;

    public string SetupPackagePath { get; set; } = "/setup";

    public string? LicenseKey { get; set; }

    public string? ApiKey { get; set; }

    public string? RavenDbS6Service { get; set; }

    [Url]
    public string? AiApiUrl { get; set; }

    [Url]
    public string? TelegramApiUrl { get; set; }

    // min interval between editMessageText calls while streaming a reply
    public TimeSpan TelegramEditDebounce { get; set; } = TimeSpan.FromSeconds(1);

    // upper bound on how long a channel document change takes to reach the running bots; a save
    // also wakes the manager, so this only backstops changes the wake missed
    public TimeSpan TelegramApplyChangesInterval { get; set; } = TimeSpan.FromSeconds(30);

    // messages held per Telegram chat while a turn runs; the sender is told to wait once the queue fills
    public int TelegramChatQueueCapacity { get; set; } = 8;

    public TimeSpan ReadinessInitialDelay { get; set; } = TimeSpan.FromSeconds(15);

    public TimeSpan ReadinessAttemptTimeout { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan ReadinessOverallTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan AiAssistTimeout { get; set; } = TimeSpan.FromMinutes(5);
}
