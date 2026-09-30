using IOPath = System.IO.Path;
// the appliance speaks RavenDB's level vocabulary; the implicit usings would bind Microsoft's
using LogLevel = Sparrow.Logging.LogLevel;

namespace Raven.Quill.Logging;

/// <summary>
/// Everything the appliance can be told about logging. An XML configuration is the whole truth when
/// there is one; otherwise a level and the two directories are all there is to say. Nothing changes any
/// of it while the process runs, so a change here takes effect on the next restart.
/// </summary>
public sealed class LogOptions
{
    private const string DefaultConfigDirectory = "/var/lib/quill";
    private const string DefaultLogDirectoryName = "logs";

    /// <summary>
    /// The XML configuration to load. Null falls back to <see cref="ConventionalConfigPath"/>, so an
    /// operator's own copy on the volume is picked up without any variable being set.
    /// </summary>
    public string? ConfigPath { get; set; }

    /// <summary>
    /// Where <c>quill.log</c> is written. Null - the default - keeps the file sink off entirely, since
    /// stdout is the appliance's primary log and s6 already rotates that stream.
    /// </summary>
    public string? Path { get; set; }

    /// <summary>
    /// Where <c>quill.audit.log</c> is written. Null keeps the audit log off, the way RavenDB reads
    /// <c>Security.AuditLog.FolderPath</c>: naming a directory is what switches it on.
    /// </summary>
    public string? AuditPath { get; set; }

    /// Null falls back to <see cref="RavenLogManagerQuillExtensions.DefaultMinLevel"/>.
    public LogLevel? MinLevel { get; set; }

    /// The file target needs a name even while its sink is off, so an unconfigured path still resolves.
    internal string ResolvedPath => Path ?? DefaultDirectory;

    internal string ResolvedAuditPath => AuditPath ?? DefaultDirectory;

    /// Where an operator's own configuration goes on the volume, loaded without needing a variable set.
    internal static string ConventionalConfigPath =>
        IOPath.Combine(DefaultConfigDirectory, RavenLogManagerQuillExtensions.ConfigFileName);

    private static string DefaultDirectory => IOPath.Combine(AppContext.BaseDirectory, DefaultLogDirectoryName);
}
