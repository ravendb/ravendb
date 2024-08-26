using NLog;
using System;
using Sparrow.Global;
using Sparrow.Logging;
using NLog.Config;
using NLog.Targets.Wrappers;
using NLog.Targets;
using Raven.Server.Config.Settings;
using LogLevel = Sparrow.Logging.LogLevel;

namespace Voron.Recovery.Logging;

internal static class RavenLogManagerVoronRecoveryExtensions
{
    public static RavenLogger GetLoggerForVoronRecovery<T>(this RavenLogManager logManager) => GetLoggerForVoronRecovery(logManager, typeof(T));

    public static RavenLogger GetLoggerForVoronRecovery(this RavenLogManager logManager, Type type)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        return new RavenLogger(LogManager.GetLogger(type.FullName)
            .WithProperty(Constants.Logging.Properties.Resource, "Voron Recovery"));
    }

    public static void ConfigureLogging(this RavenLogManager logManager, LogLevel logLevel, PathSetting path)
    {
        var fileTarget = new FileTarget
        {
            Name = nameof(FileTarget),
            CreateDirs = true,
            FileName = path.Combine("recovery.${shortdate}.log").FullPath,
            ArchiveNumbering = ArchiveNumberingMode.DateAndSequence,
            Header = Constants.Logging.DefaultHeaderAndFooterLayout,
            Layout = Constants.Logging.DefaultLayout,
            Footer = Constants.Logging.DefaultHeaderAndFooterLayout,
            ConcurrentWrites = false,
        };

        var fileTargetAsyncWrapper = new AsyncTargetWrapper(nameof(AsyncTargetWrapper), fileTarget);

        var defaultRule = new LoggingRule("*", logLevel.ToNLogLogLevel(), NLog.LogLevel.Fatal, fileTargetAsyncWrapper)
        {
            RuleName = "Raven_Default"
        };

        var config = new LoggingConfiguration();

        config.AddRule(defaultRule);

        LogManager.Setup(x => x.LoadConfiguration(config));
        LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);
    }
}
