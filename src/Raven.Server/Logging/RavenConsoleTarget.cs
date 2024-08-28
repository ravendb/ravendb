using System.Diagnostics;
using System.Linq;
using NLog;
using LogLevel = NLog.LogLevel;

namespace Raven.Server.Logging;

public static class RavenConsoleTarget
{
    public static void Enable()
    {
        var defaultRule = RavenLogManagerServerExtensions.DefaultRule;

        var minLevel = defaultRule.Levels.FirstOrDefault() ?? LogLevel.Trace;
        var maxLevel = defaultRule.Levels.LastOrDefault() ?? LogLevel.Fatal;
        RavenLogManagerServerExtensions.ConsoleRule.SetLoggingLevels(minLevel, maxLevel);

        var configuration = LogManager.Configuration;

        Debug.Assert(configuration != null, "configuration != null");
        Debug.Assert(configuration.FindRuleByName(RavenLogManagerServerExtensions.ConsoleRule.RuleName) == null, $"configuration.FindRuleByName({RavenLogManagerServerExtensions.ConsoleRule.RuleName}) == null");

        LogManager.Configuration.AddRule(RavenLogManagerServerExtensions.ConsoleRule);

        LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);
    }

    public static void Disable()
    {
        RavenLogManagerServerExtensions.ConsoleRule.DisableLoggingForLevels(LogLevel.Trace, LogLevel.Fatal);

        var configuration = LogManager.Configuration;

        Debug.Assert(configuration != null, "configuration != null");

        if (configuration.RemoveRuleByName(RavenLogManagerServerExtensions.ConsoleRule.RuleName))
            LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);
    }
}
