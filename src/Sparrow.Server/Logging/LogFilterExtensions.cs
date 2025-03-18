using System;
using NLog.Filters;
using Sparrow.Logging;

namespace Sparrow.Server.Logging;

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
