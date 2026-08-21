using NLog;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;
using NLog.Targets.Wrappers;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using LogLevel = NLog.LogLevel;

namespace Raven.Quill.Logging;

// Raven.Quill.Constants would otherwise shadow this for every file in a Raven.Quill.* sub-namespace,
// and an alias only outranks an enclosing namespace type from inside the namespace body.
using Constants = Sparrow.Global.Constants;

/// <summary>
/// Quill's half of <see cref="RavenLogManager"/>, the counterpart of RavenDB's
/// RavenLogManagerServerExtensions: it owns the rules NLog was configured with and hands out the loggers
/// that carry Quill's resource name. Logging is configured once, from <see cref="ConfigureLogging"/>, and
/// read-only afterwards. The state below is static because NLog's LogManager is, exactly as in RavenDB.
/// </summary>
internal static class RavenLogManagerQuillExtensions
{
    /// The only name LogOptions needs: where an operator's own copy goes on the volume.
    internal const string ConfigFileName = "quill.nlog.config";

    private const string TemplateFileName = "quill.nlog.template.config";
    private const string NormalFileName = "quill.log";
    private const string AuditFileName = "quill.audit.log";

    private const string NormalTargetName = "QuillLogging";
    private const string AuditTargetName = "QuillLoggingAudit";
    private const string AsyncWrapperName = "AsyncTargetWrapper";
    private const string AuditAsyncWrapperName = "AuditAsyncTargetWrapper";
    private const string ConsoleTargetName = "Console";
    private const string AuditLoggerName = "Audit";
    private const string PollyRuleName = "Raven_Polly";
    private const string PollyLoggerNamePattern = "Polly*";

    private const string QuillResourceName = "Quill";
    private const string WriteProbeFileName = "write.test";

    private const string Layout = Constants.Logging.DefaultLayout;
    private const string HeaderAndFooterLayout = Constants.Logging.DefaultHeaderAndFooterLayout;

    private const long ArchiveAboveSizeInBytes = 128 * Constants.Size.Megabyte;
    private const int MaxArchiveDays = 3;
    private const bool EnableArchiveFileCompression = false;

    internal const Sparrow.Logging.LogLevel DefaultMinLevel = Sparrow.Logging.LogLevel.Info;

    private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForQuill<RavenLogManager>();

    private static LoggingRule DefaultRule;

    private static LoggingRule DefaultAuditRule;

    private static RavenAuditLogger AuditLogger;

    public static RavenLogger GetLoggerForQuill<T>(this RavenLogManager logManager) =>
        GetLoggerForQuill(logManager, typeof(T));

    public static RavenLogger GetLoggerForQuill(this RavenLogManager logManager, Type type)
    {
        ArgumentNullException.ThrowIfNull(type);

        return new RavenLogger(LogManager.GetLogger(type.FullName)
            .WithProperty(Constants.Logging.Properties.Resource, QuillResourceName));
    }

    public static RavenAuditLogger GetAuditLoggerForQuill(this RavenLogManager logManager) =>
        AuditLogger ??= new RavenAuditLogger(LogManager.GetLogger(AuditLoggerName)
            .WithProperty(Constants.Logging.Properties.Resource, QuillResourceName));

    /// <summary>
    /// Loads the XML configuration when there is one, and otherwise builds the built-in defaults from
    /// <paramref name="options"/>. A file that was named but is not there, or that has lost one of the
    /// rules the appliance looks up, is an error rather than a fallback - the same assertions RavenDB
    /// makes, so what an operator asked for is either in force or the reason is on the way out.
    /// </summary>
    public static void ConfigureLogging(this RavenLogManager logManager, LogOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (TryConfigureLoggingFromFile(options))
            return;

        var minLevel = options.MinLevel ?? DefaultMinLevel;

        var fileSink = new AsyncTargetWrapper(AsyncWrapperName,
            NewFileTarget(NormalTargetName, Path.Combine(options.ResolvedPath, NormalFileName)));
        var auditSink = new AsyncTargetWrapper(AuditAsyncWrapperName,
            NewFileTarget(AuditTargetName, Path.Combine(options.ResolvedAuditPath, AuditFileName)));
        var consoleTarget = new ConsoleTarget(ConsoleTargetName) { Layout = new SimpleLayout(Layout) };

        // Polly's retry chatter is dropped before it reaches any other rule
        var pollyRule = new LoggingRule(PollyLoggerNamePattern, LogLevel.Trace, LogLevel.Fatal,
            new NullTarget(PollyRuleName))
        {
            RuleName = PollyRuleName,
            Final = true,
        };

        // stdout is the appliance's primary log and s6 already rotates it, so the file sink is attached
        // only when a directory was named for it
        DefaultRule = new LoggingRule("*", minLevel.ToNLogLogLevel(), minLevel.ToNLogMaxLogLevel(), consoleTarget)
        {
            RuleName = Constants.Logging.Names.DefaultRuleName,
        };

        if (SinkRequested(options.Path, "log"))
            DefaultRule.Targets.Add(fileSink);

        // naming a directory is the switch, the way RavenDB reads Security.AuditLog.FolderPath
        DefaultAuditRule = new LoggingRule(AuditLoggerName, LogLevel.Info, LogLevel.Info,
            SinkRequested(options.AuditPath, "audit log")
                ? auditSink
                : new NullTarget(nameof(NullTarget)))
        {
            RuleName = Constants.Logging.Names.DefaultAuditRuleName,
            Final = true,
        };

        var config = new LoggingConfiguration();

        config.AddRule(pollyRule);
        config.AddRule(DefaultAuditRule);
        config.AddRule(DefaultRule);

        SetAudit();

        LogManager.Setup(x => x.LoadConfiguration(config));
        LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);

        if (Logger.IsInfoEnabled)
            Logger.Info(
                $"Logging set to [{minLevel}, {minLevel.ToNLogMaxLogLevel()}] level, writing to " +
                $"{DescribeSinks(options)}.");

        AuditStartup();

        static bool TryConfigureLoggingFromFile(LogOptions options)
        {
            // the volume's own copy is picked up without a variable set, and its absence is normal; a
            // file that was named, on the other hand, has to be there
            var configPath = options.ConfigPath;

            if (configPath is null)
            {
                if (File.Exists(LogOptions.ConventionalConfigPath) == false)
                    return false;

                configPath = LogOptions.ConventionalConfigPath;
            }

            LogManager.Setup(x => x.LoadConfigurationFromFile(configPath, optional: false));

            var c = LogManager.Configuration;

            DefaultRule = c.FindRuleByName(Constants.Logging.Names.DefaultRuleName);
            if (DefaultRule == null)
                ThrowNoRule(Constants.Logging.Names.DefaultRuleName);

            DefaultAuditRule = c.FindRuleByName(Constants.Logging.Names.DefaultAuditRuleName);
            if (DefaultAuditRule == null)
                ThrowNoRule(Constants.Logging.Names.DefaultAuditRuleName);

            SetAudit();

            LogManager.Setup(x => x.LoadConfiguration(c));
            LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);

            if (Logger.IsInfoEnabled)
                Logger.Info(
                    $"Logging configured from '{configPath}' configuration file and set to " +
                    $"[{DefaultRule.Levels.FirstOrDefault() ?? LogLevel.Off}, " +
                    $"{DefaultRule.Levels.LastOrDefault() ?? LogLevel.Off}] level.");

            AuditStartup();

            return true;
        }
    }

    private static void SetAudit() =>
        RavenLogManager.SetAudit(DefaultAuditRule.Targets.Count > 0 &&
                                DefaultAuditRule.Targets.Any(t => t.GetType() != typeof(NullTarget)));

    /// The audit log's own first line, the way RavenDB opens one when it configures the audit target.
    private static void AuditStartup()
    {
        // the cached logger belongs to whatever configuration was in place before this one
        AuditLogger = null;

        var logger = new QuillLogger<RavenLogManager>();

        if (logger.AuditEnabled)
            logger.Audit("AUDIT", "log started", context: null);
    }

    private static string DescribeSinks(LogOptions options)
    {
        var sinks = new List<string> { "the console" };

        if (options.Path is { } path)
            sinks.Add($"'{Path.Combine(path, NormalFileName)}'");

        if (options.AuditPath is { } auditPath)
            sinks.Add($"'{Path.Combine(auditPath, AuditFileName)}' (audit)");

        return string.Join(", ", sinks);
    }

    private static FileTarget NewFileTarget(string name, string fileName) => new(name)
    {
        CreateDirs = true,
        FileName = new SimpleLayout(fileName),
        ArchiveNumbering = ArchiveNumberingMode.DateAndSequence,
        Header = new SimpleLayout(HeaderAndFooterLayout),
        Layout = new SimpleLayout(Layout),
        Footer = new SimpleLayout(HeaderAndFooterLayout),
        ConcurrentWrites = false,
        WriteFooterOnArchivingOnly = true,
        ArchiveAboveSize = ArchiveAboveSizeInBytes,
        ArchiveOldFileOnStartup = true,
        ArchiveOldFileOnStartupAboveSize = ArchiveAboveSizeInBytes,
        MaxArchiveDays = MaxArchiveDays,
        EnableArchiveFileCompression = EnableArchiveFileCompression,
    };

    /// <summary>
    /// Whether a sink was asked for at all - null means it was not. One that was asked for and cannot be
    /// written to is an error, probed here rather than on the first write: NLog swallows a write failure,
    /// so the sink would otherwise silently do nothing at all.
    /// </summary>
    private static bool SinkRequested(string directory, string what)
    {
        if (directory is null)
            return false;

        try
        {
            Directory.CreateDirectory(directory);
            var probe = Path.Combine(directory, WriteProbeFileName);
            File.WriteAllText(probe, "test we can write");
            File.Delete(probe);
        }
        catch (Exception e)
        {
            throw new InvalidOperationException(
                $"the {what} directory '{directory}' could not be created or written to.", e);
        }

        return true;
    }

    private static void ThrowNoRule(string ruleName) =>
        throw new InvalidOperationException(
            $"Could not find the '{ruleName}' rule in the logging configuration. Rules are looked up by " +
            $"name, so they must keep the names the shipped {TemplateFileName} gives them.");
}
