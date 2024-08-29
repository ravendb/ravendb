using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using JetBrains.Annotations;
using NLog;
using NLog.Config;
using NLog.Targets;
using Sparrow.Collections;

namespace Raven.Server.Logging;

public class AbstractTarget : TargetWithLayout
{
    private static readonly object Locker = new();

    protected class TargetCountGuardian
    {
        private int _registeredSockets;

        public int Increment()
        {
            return Interlocked.Increment(ref _registeredSockets);
        }

        public int Decrement()
        {
            return Interlocked.Decrement(ref _registeredSockets);
        }
    }

    protected static IDisposable RegisterInternal<T>(LoggingRule loggingRule, T item, ConcurrentSet<T> items, TargetCountGuardian registeredItems)
    {
        if (items.TryAdd(item) == false)
            throw new InvalidOperationException("Item was already added?");

        var registeredSockets = registeredItems.Increment();
        if (registeredSockets == 1)
        {
            lock (Locker)
            {
                var defaultRule = RavenLogManagerServerExtensions.DefaultRule;
                var minLevel = defaultRule.Levels.FirstOrDefault() ?? LogLevel.Trace;
                var maxLevel = defaultRule.Levels.LastOrDefault() ?? LogLevel.Fatal;

                loggingRule.SetLoggingLevels(minLevel, maxLevel);

                var configuration = LogManager.Configuration;

                Debug.Assert(configuration != null, "configuration != null");
                Debug.Assert(configuration.FindRuleByName(loggingRule.RuleName) == null, $"configuration.FindRuleByName({loggingRule.RuleName}) == null");

                LogManager.Configuration.AddRule(loggingRule);

                LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);
            }
        }

        return new ReleaseItem<T>(loggingRule, item, items, registeredItems);
    }

    protected static void CopyToStream(StringBuilder builder, MemoryStream ms, Encoding encoding, char[] transformBuffer)
    {
        int byteCount = encoding.GetMaxByteCount(builder.Length);
        long position = ms.Position;
        ms.SetLength(position + byteCount);
        for (int i = 0; i < builder.Length; i += transformBuffer.Length)
        {
            var charCount = Math.Min(builder.Length - i, transformBuffer.Length);
            builder.CopyTo(i, transformBuffer, 0, charCount);
            byteCount = encoding.GetBytes(transformBuffer, 0, charCount, ms.GetBuffer(), (int)position);
            position += byteCount;
        }
        ms.Position = position;
        if (position != ms.Length)
        {
            ms.SetLength(position);
        }
    }

    private class ReleaseItem<T> : IDisposable
    {
        private readonly LoggingRule _loggingRule;
        private readonly T _item;
        private readonly ConcurrentSet<T> _items;
        private readonly TargetCountGuardian _registeredItems;

        public ReleaseItem([NotNull] LoggingRule loggingRule, [NotNull] T item, [NotNull] ConcurrentSet<T> items, TargetCountGuardian registeredItems)
        {
            _loggingRule = loggingRule ?? throw new ArgumentNullException(nameof(loggingRule));
            _item = item ?? throw new ArgumentNullException(nameof(item));
            _items = items ?? throw new ArgumentNullException(nameof(items));
            _registeredItems = registeredItems;
        }

        public void Dispose()
        {
            _items.TryRemove(_item);

            var counter = _registeredItems.Decrement();
            if (counter != 0)
                return;

            lock (Locker)
            {
                _loggingRule.DisableLoggingForLevels(LogLevel.Trace, LogLevel.Fatal);

                var configuration = LogManager.Configuration;

                Debug.Assert(configuration != null, "configuration != null");
                Debug.Assert(configuration.FindRuleByName(_loggingRule.RuleName) != null, $"configuration.FindRuleByName({_loggingRule.RuleName}) != null");

                if (configuration.RemoveRuleByName(_loggingRule.RuleName))
                    LogManager.ReconfigExistingLoggers(purgeObsoleteLoggers: true);
            }
        }
    }
}
