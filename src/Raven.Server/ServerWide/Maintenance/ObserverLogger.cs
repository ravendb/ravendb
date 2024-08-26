using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.ServerWide.Maintenance
{
    public sealed class ObserverLogger
    {
        private readonly RavenLogger _logger;
        private readonly BlockingCollection<ClusterObserverLogEntry> _decisionsLog;
        private readonly Dictionary<string, long> _lastLogs;

        public BlockingCollection<ClusterObserverLogEntry> DecisionsLog => _decisionsLog;

        public ObserverLogger(string nodeTag)
        {
            _logger = RavenLogManager.Instance.GetLoggerForCluster<ObserverLogger>(LoggingComponent.NodeTag(nodeTag));
            _lastLogs = new Dictionary<string, long>();
            _decisionsLog = new BlockingCollection<ClusterObserverLogEntry>();
        }

        public void Log(string message, long iteration, Exception e = null, string database = null)
        {
            if (iteration % 10_000 == 0)
                _lastLogs.Clear();

            if (_lastLogs.TryGetValue(message, out var last))
            {
                if (last + 60 > iteration)
                    // each iteration occur every 500 ms, so we update the log with the _same_ message every 30 sec (60 * 0.5s)
                    return;
            }
            _lastLogs[message] = iteration;
            AddToDecisionLog(database, message, iteration, e);

            if (_logger.IsInfoEnabled)
            {
                _logger.Info(message, e);
            }
        }

        public void AddToDecisionLog(string database, string updateReason, long iteration, Exception e)
        {
            if (e != null)
                updateReason += $"{Environment.NewLine}Error: {e}";

            AddToDecisionLog(database, updateReason, iteration);
        }

        public void AddToDecisionLog(string database, string updateReason, long iteration)
        {
            if (_decisionsLog.Count > 99)
                _decisionsLog.Take();

            _decisionsLog.Add(new ClusterObserverLogEntry
            {
                Database = database,
                Iteration = iteration,
                Message = updateReason,
                Date = DateTime.UtcNow
            });
        }
    }

}
