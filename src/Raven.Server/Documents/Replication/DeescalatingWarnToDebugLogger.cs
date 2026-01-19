using System;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Replication
{
    /// <summary>
    /// Logger that de-escalates from WARN to DEBUG after the first log entry.
    /// Useful for reducing log noise from recurring transient errors.
    /// </summary>
    public sealed class DeescalatingWarnToDebugLogger
    {
        private readonly RavenLogger _logger;

        // Tracks whether the first log entry has been written for this logger instance
        private bool _logged;

        /// <summary>
        /// Creates a new de-escalating logger.
        /// </summary>
        /// <param name="logger">The underlying logger to use</param>
        public DeescalatingWarnToDebugLogger(RavenLogger logger)
        {
            ArgumentNullException.ThrowIfNull(logger);

            _logger = logger;
        }

        /// <summary>
        /// Returns true if logging is enabled at the current level.
        /// For the first log entry, checks WARN level; for subsequent entries, checks DEBUG level.
        /// </summary>
        public bool IsEnabled => _logged == false ? _logger.IsWarnEnabled : _logger.IsDebugEnabled;

        /// <summary>
        /// Logs a message with an exception. First call logs at WARN level, 
        /// subsequent calls log at DEBUG level.
        /// </summary>
        /// <param name="message">The message to log</param>
        /// <param name="exception">The exception to log</param>
        public void Log(string message, Exception exception)
        {
            if (_logged)
            {
                if (_logger.IsDebugEnabled)
                    _logger.Debug(message, exception);
            }
            else
            {
                _logged = true;

                if (_logger.IsWarnEnabled)
                    _logger.Warn(message, exception);
            }
        }
    }
}
