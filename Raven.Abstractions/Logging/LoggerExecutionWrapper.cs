using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Sparrow.Collections;

namespace Raven.Abstractions.Logging
{
    public class LoggerExecutionWrapper : ILog
    {
        public const string FailedToGenerateLogMessage = "Failed to generate log message";
        private readonly ILog _logger;
        private readonly string _loggerName;
        private IReadOnlyList<Target> _targets;
        private readonly object _targetsUpdateSyncObj = new object();

        public LoggerExecutionWrapper(ILog logger, string loggerName, IReadOnlyList<Target> targets)
        {
            this._logger = logger;
            this._loggerName = loggerName;
            this._targets = targets;
        }

        public ILog WrappedLogger
        {
            get { return _logger; }
        }

        #region ILog Members

        public bool IsInfoEnabled
        {
            get { return LogManager.EnableDebugLogForTargets || _logger.IsInfoEnabled; }
        }

        public bool IsDebugEnabled
        {
            get { return LogManager.EnableDebugLogForTargets || _logger.IsDebugEnabled; }
        }

        public bool IsWarnEnabled
        {
            get { return LogManager.EnableDebugLogForTargets || _logger.IsWarnEnabled; }
        }

        public void HandleTargetsChange(IReadOnlyList<Target> targets)
        {
            lock (_targetsUpdateSyncObj)
            {
                _targets = targets;
            }
        }

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

            var shouldLog = false;
            lock (_targetsUpdateSyncObj)
            {
                if (_targets.Count == 0)
                    return;
                
                // ReSharper disable once ForCanBeConvertedToForeach
                for (var i = 0; i < _targets.Count; i++)
                {
                    var target = _targets[i];
                    shouldLog |= target.ShouldLog(_logger, logLevel);
                }
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
#if !DNXCORE50
            string databaseName = LogContext.ResourceName;
            if (string.IsNullOrWhiteSpace(databaseName))
                databaseName = Constants.SystemDatabase;
#else
            var databaseName = Constants.SystemDatabase;
#endif

            foreach (var target in _targets)
            {
                target.Write(new LogEventInfo
                {
                    Database = databaseName,
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
