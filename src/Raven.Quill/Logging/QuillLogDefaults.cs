using NLog;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;
using NLog.Targets.Wrappers;
using Sparrow.Global;
using Sparrow.Server.Logging;
using LogLevel = Sparrow.Logging.LogLevel;
using NLogLevel = NLog.LogLevel;

namespace Raven.Quill.Logging;
internal static class QuillLogDefaults
{
    internal const string Layout = Constants.Logging.DefaultLayout;

    internal const string HeaderAndFooterLayout = Constants.Logging.DefaultHeaderAndFooterLayout;

    internal const string PollyRuleName = "Raven_Polly";
    internal const string PollyLoggerNamePattern = "Polly*";
    internal const string ConsoleTargetName = "Console";

    internal const long ArchiveAboveSizeInBytes = 128 * Constants.Size.Megabyte;
    internal const int MaxArchiveDays = 3;
    internal const bool EnableArchiveFileCompression = false;

    internal const LogLevel MinLevel = LogLevel.Info;
    internal const LogLevel MicrosoftMinLevel = LogLevel.Off;

    internal static LoggingConfiguration Build(NLog.LogFactory factory, QuillLogOptions options)
    {
        var configuration = new LoggingConfiguration(factory);

        var fileTarget = File(QuillLogging.NormalTargetName,
            Path.Combine(options.Path, QuillLogging.NormalFileName), autoFlush: false);
        var fileSink = new AsyncTargetWrapper(QuillLogging.AsyncWrapperName, fileTarget);
        var auditTarget = File(QuillLogging.AuditTargetName,
            Path.Combine(options.AuditPath, QuillLogging.AuditFileName), autoFlush: true);
        var consoleTarget = new ConsoleTarget(ConsoleTargetName) { Layout = new SimpleLayout(Layout) };
        var pollyTarget = new NullTarget(PollyRuleName);

        configuration.AddTarget(fileTarget);
        configuration.AddTarget(fileSink);
        configuration.AddTarget(auditTarget);
        configuration.AddTarget(consoleTarget);
        configuration.AddTarget(pollyTarget);

        configuration.AddRule(new LoggingRule(PollyLoggerNamePattern, NLogLevel.Trace, NLogLevel.Fatal, pollyTarget)
        {
            RuleName = PollyRuleName,
            Final = true,
        });

        configuration.AddRule(new LoggingRule
        {
            RuleName = Constants.Logging.Names.SystemRuleName,
            LoggerNamePattern = "System.*",
            FinalMinLevel = MicrosoftMinLevel.ToNLogFinalMinLogLevel(),
        });

        configuration.AddRule(new LoggingRule
        {
            RuleName = Constants.Logging.Names.MicrosoftRuleName,
            LoggerNamePattern = "Microsoft.*",
            FinalMinLevel = MicrosoftMinLevel.ToNLogFinalMinLogLevel(),
        });

        var auditRule = new LoggingRule { RuleName = Constants.Logging.Names.DefaultAuditRuleName, Final = true };
        auditRule.LoggerNamePattern = QuillLogging.AuditLoggerName;
        auditRule.EnableLoggingForLevel(NLogLevel.Info);
        configuration.AddRule(auditRule);

        // stdout is the appliance's primary log and s6 already rotates it, so the file sink stays off
        configuration.AddRule(new LoggingRule("*", MinLevel.ToNLogLogLevel(), MinLevel.ToNLogMaxLogLevel(), consoleTarget)
        {
            RuleName = Constants.Logging.Names.DefaultRuleName,
        });

        return configuration;
    }

    private static FileTarget File(string name, string fileName, bool autoFlush) => new(name)
    {
        CreateDirs = true,
        FileName = new SimpleLayout(fileName),
        ArchiveNumbering = ArchiveNumberingMode.DateAndSequence,
        Header = new SimpleLayout(HeaderAndFooterLayout),
        Layout = new SimpleLayout(Layout),
        Footer = new SimpleLayout(HeaderAndFooterLayout),
        ConcurrentWrites = false,
        WriteFooterOnArchivingOnly = true,
        AutoFlush = autoFlush,
        ArchiveAboveSize = ArchiveAboveSizeInBytes,
        ArchiveOldFileOnStartup = true,
        ArchiveOldFileOnStartupAboveSize = ArchiveAboveSizeInBytes,
        MaxArchiveDays = MaxArchiveDays,
        EnableArchiveFileCompression = EnableArchiveFileCompression,
    };
}
