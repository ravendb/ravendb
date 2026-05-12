using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Sparrow.Logging;

namespace Raven.Server.ServerWide.Maintenance
{
    public sealed class ObserverLogger
    {
        private readonly BlockingCollection<ClusterObserverLogEntry> _decisionsLog;
        private readonly Dictionary<string, long> _lastLogs;

        public Logger Logger { get; }

        public BlockingCollection<ClusterObserverLogEntry> DecisionsLog => _decisionsLog;

        public ObserverLogger(string nodeTag)
        {
            Logger = LoggingSource.Instance.GetLogger<ClusterObserver>(nodeTag);
            _lastLogs = new Dictionary<string, long>();
            _decisionsLog = new BlockingCollection<ClusterObserverLogEntry>();
        }

        /// <summary>
        /// Records a recurring message to the in-memory decision log and the server log.
        /// This method throttles identical messages, logging them at most once every 30 seconds.
        /// </summary>
        /// <param name="message">The message to log.</param>
        /// <param name="iteration">The current cluster observer iteration.</param>
        /// <param name="e">An optional exception associated with the log message.</param>
        /// <param name="database">The database name this log applies to, if any.</param>
        public void Log(string message, long iteration, Exception e = null, string database = null)
        {
            if (iteration % 10_000 == 0)
                _lastLogs.Clear();

            if (_lastLogs.TryGetValue(message, out var last))
            {
                if (last + 60 > iteration)
                    // each iteration occurs every 500 ms, so we update the log with the _same_ message every 30 sec (60 * 0.5s)
                    return;
            }
            _lastLogs[message] = iteration;
            
            AddToDecisionLog(database, message, iteration, e);
        }

        /// <summary>
        /// Unconditionally records a cluster decision to the in-memory log.
        /// If Information-level logging is enabled, the decision is also written to the server log.
        /// </summary>
        /// <param name="database">The database name associated with the decision.</param>
        /// <param name="updateReason">The explanation of the decision made by the cluster.</param>
        /// <param name="iteration">The current cluster observer iteration.</param>
        /// <param name="e">An optional exception associated with the decision.</param>
        public void AddToDecisionLog(string database, string updateReason, long iteration, Exception e = null)
        {
            if (Logger.IsInfoEnabled)
            {
                var prefix = string.IsNullOrWhiteSpace(database) ? string.Empty : $"Database '{database}' : ";
                var decisionPrefix = e == null ? "Decision: " : string.Empty;
                Logger.Info($"{prefix}{decisionPrefix}{updateReason}", e);
            }

            if (e != null)
                updateReason += $"{Environment.NewLine}Error: {e}";

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
