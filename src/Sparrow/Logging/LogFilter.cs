using System;
using NLog.Filters;

namespace Sparrow.Logging;

public sealed class LogFilter
{
    internal LogFilter()
    {
        // for deserialization
    }

    public LogFilter(LogLevel minLevel, LogLevel maxLevel, string condition, LogFilterAction action)
    {
        MinLevel = minLevel;
        MaxLevel = maxLevel;
        Condition = condition ?? throw new ArgumentNullException(nameof(condition));
        Action = action;
    }

    public LogLevel MinLevel { get; internal set; }

    public LogLevel MaxLevel { get; internal set; }

    public string Condition { get; internal set; }

    public LogFilterAction Action { get; internal set; }
}

internal static class LogFilterExtensions
{
    public static FilterResult ToNLogFilterResult(this LogFilterAction action)
    {
        switch (action)
        {
            case LogFilterAction.Neutral:
                return FilterResult.Neutral;
            case LogFilterAction.Log:
                return FilterResult.Log;
            case LogFilterAction.Ignore:
                return FilterResult.Ignore;
            case LogFilterAction.LogFinal:
                return FilterResult.LogFinal;
            case LogFilterAction.IgnoreFinal:
                return FilterResult.IgnoreFinal;
            default:
                throw new ArgumentOutOfRangeException(nameof(action), action, null);
        }
    }
}
