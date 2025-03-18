using System;
using JetBrains.Annotations;
using NLog;
using NLog.Filters;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Logging;

public sealed class RavenConditionBasedFilter : ConditionBasedFilter
{
    public readonly LogFilter Filter;

    private readonly NLog.LogLevel _minLevel;
    private readonly NLog.LogLevel _maxLevel;

    public RavenConditionBasedFilter([NotNull] LogFilter filter)
    {
        Filter = filter ?? throw new ArgumentNullException(nameof(filter));

        _minLevel = filter.MinLevel.ToNLogLogLevel();
        _maxLevel = filter.MaxLevel.ToNLogLogLevel();
        Action = filter.Action.ToNLogFilterResult();

        try
        {
            Condition = filter.Condition;
        }
        catch (Exception e)
        {
            throw new InvalidOperationException($"Could not parse '{filter.Condition}' expression.", e);
        }
    }

    protected override FilterResult Check(LogEventInfo logEvent)
    {
        if (logEvent.Level < _minLevel)
            return FilterResult.Neutral;

        if (logEvent.Level > _maxLevel)
            return FilterResult.Neutral;

        return base.Check(logEvent);
    }
}
