using System;
using NLog.Filters;
using Sparrow.Json.Parsing;

namespace Sparrow.Logging;

public sealed class LogFilter : IDynamicJson
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

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(MinLevel)] = MinLevel,
            [nameof(MaxLevel)] = MaxLevel,
            [nameof(Condition)] = Condition,
            [nameof(LogFilterAction)] = Action
        };
    }
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

    public static LogFilterAction ToLogFilterAction(this FilterResult result)
    {
        switch (result)
        {
            case FilterResult.Neutral:
                return LogFilterAction.Neutral;
            case FilterResult.Log:
                return LogFilterAction.Log;
            case FilterResult.Ignore:
                return LogFilterAction.Ignore;
            case FilterResult.LogFinal:
                return LogFilterAction.LogFinal;
            case FilterResult.IgnoreFinal:
                return LogFilterAction.IgnoreFinal;
            default:
                throw new ArgumentOutOfRangeException(nameof(result), result, null);
        }
    }
}
