using System.ComponentModel.DataAnnotations;
using Raven.Quill.Logging;

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

    public string SetupNodeSettingsPath => Path.Combine(SetupPackagePath, "A", "settings.json");

    public string? LicenseKey { get; set; }

    public string? ApiKey { get; set; }

    public string? RavenDbS6Service { get; set; }

    /// <summary>
    /// Base URL of the AI Helper service; unset, the RavenDB URL already connected to is used. No
    /// environment variable sets this - it is the seam the tests point at their AI Helper mock.
    /// </summary>
    [Url]
    public string? AiApiUrl { get; set; }

    public TelegramOptions Telegram { get; set; } = new();

    /// <summary>
    /// What the appliance was told about logging. Parsed and rejected on the way in, the way the readiness
    /// timeouts are, so what lands here is already known good.
    /// </summary>
    public LogOptions Logs { get; set; } = new();

    public SlackOptions Slack { get; set; } = new();

    public DiscordOptions Discord { get; set; } = new();

    public TimeSpan ReadinessInitialDelay { get; set; } = TimeSpan.FromSeconds(15);

    public TimeSpan ReadinessAttemptTimeout { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan ReadinessOverallTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan AiAssistTimeout { get; set; } = TimeSpan.FromMinutes(5);
}
