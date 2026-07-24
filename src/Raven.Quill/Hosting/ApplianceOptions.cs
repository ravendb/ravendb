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

    public const string DefaultLicenseApiUrl = "https://api.ravendb.net";

    public string LicenseApiUrl { get; set; } = DefaultLicenseApiUrl;

    [Url]
    public string? AiApiUrl { get; set; }

    public TimeSpan ReadinessInitialDelay { get; set; } = TimeSpan.FromSeconds(15);

    public TimeSpan ReadinessAttemptTimeout { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan ReadinessOverallTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan AiAssistTimeout { get; set; } = TimeSpan.FromMinutes(5);
}
