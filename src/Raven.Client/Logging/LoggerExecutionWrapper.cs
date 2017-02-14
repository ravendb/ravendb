using System;
using Raven.Client.Util;
using Sparrow.Collections;

namespace Raven.Client.Logging
{
    internal class LoggerExecutionWrapper : ILog
    {
        public const string FailedToGenerateLogMessage = "Failed to generate log message";
        private readonly ILog _logger;
        private readonly string _loggerName;
        private readonly ConcurrentSet<Target> _targets;

        public LoggerExecutionWrapper(ILog logger, string loggerName, ConcurrentSet<Target> targets)
        {
            _logger = logger;
            _loggerName = loggerName;
            _targets = targets;
        }

        public ILog WrappedLogger => _logger;

        #region ILog Members

        public bool IsInfoEnabled => LogManager.EnableDebugLogForTargets || _logger.IsInfoEnabled;

        public bool IsDebugEnabled => LogManager.EnableDebugLogForTargets || _logger.IsDebugEnabled;

        public bool IsWarnEnabled => LogManager.EnableDebugLogForTargets || _logger.IsWarnEnabled;

        public void Log(LogLevel logLevel, Func<string> messageFunc)
        {
            if (_logger.ShouldLog(logLevel))
            {
                Func<string> wrappedMessageFunc = () =>
                {
                    try
                    {
                        return messageFunc();
                    }
                    catch (Exception ex)
                    {
                        Log(LogLevel.Error, () => FailedToGenerateLogMessage, ex);
                    }
                    return null;
                };
                _logger.Log(logLevel, wrappedMessageFunc);
            }

            if (_targets.Count == 0)
                return;
            var shouldLog = false;
            // ReSharper disable once LoopCanBeConvertedToQuery - perf
            foreach (var target in _targets)
            {
                shouldLog |= target.ShouldLog(_logger, logLevel);
            }
            if (shouldLog == false)
                return;
            string formattedMessage;
            try
            {
                formattedMessage = messageFunc();
            }
            catch (Exception)
            {
                // nothing to be done here
                return;
            }

            var resourceName = LogContext.ResourceName;
            foreach (var target in _targets)
            {
                target.Write(new LogEventInfo
                {
                    Database = resourceName,
                    Exception = null,
                    FormattedMessage = formattedMessage,
                    Level = logLevel,
                    LoggerName = _loggerName,
                    TimeStamp = SystemTime.UtcNow,
                });
            }
        }

        public void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception)
            where TException : Exception
        {
            Func<string> wrappedMessageFunc = () =>
            {
                try
                {
                    return messageFunc();
                }
                catch (Exception ex)
                {
                    Log(LogLevel.Error, () => FailedToGenerateLogMessage, ex);
                }
                return null;
            };
            _logger.Log(logLevel, wrappedMessageFunc, exception);
            foreach (var target in _targets)
            {
                target.Write(new LogEventInfo
                {
                    Exception = exception,
                    FormattedMessage = wrappedMessageFunc(),
                    Level = logLevel,
                    LoggerName = _loggerName,
                    TimeStamp = SystemTime.UtcNow,
                });
            }
        }

        public bool ShouldLog(LogLevel logLevel)
        {
            return _logger.ShouldLog(logLevel);
        }

        #endregion
    }
}
