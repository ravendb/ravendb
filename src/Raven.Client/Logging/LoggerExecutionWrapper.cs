using System;
using Raven.NewClient.Abstractions.Data;
using Sparrow.Collections;

namespace Raven.NewClient.Abstractions.Logging
{
    public class LoggerExecutionWrapper : ILog
    {
        public const string FailedToGenerateLogMessage = "Failed to generate log message";
        private readonly ILog logger;
        private readonly string loggerName;
        private readonly ConcurrentSet<Target> targets;

        public LoggerExecutionWrapper(ILog logger, string loggerName, ConcurrentSet<Target> targets)
        {
            this.logger = logger;
            this.loggerName = loggerName;
            this.targets = targets;
        }

        public ILog WrappedLogger
        {
            get { return logger; }
        }

        #region ILog Members

        public bool IsInfoEnabled
        {
            get { return LogManager.EnableDebugLogForTargets || logger.IsInfoEnabled; }
        }

        public bool IsDebugEnabled
        {
            get { return LogManager.EnableDebugLogForTargets || logger.IsDebugEnabled; }
        }

        public bool IsWarnEnabled
        {
            get { return LogManager.EnableDebugLogForTargets || logger.IsWarnEnabled; }
        }

        public void Log(LogLevel logLevel, Func<string> messageFunc)
        {
            if (logger.ShouldLog(logLevel))
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
                logger.Log(logLevel, wrappedMessageFunc);
            }

            if (targets.Count == 0)
                return;
            var shouldLog = false;
            // ReSharper disable once LoopCanBeConvertedToQuery - perf
            foreach (var target in targets)
            {
                shouldLog |= target.ShouldLog(logger, logLevel);
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
            foreach (var target in targets)
            {
                target.Write(new LogEventInfo
                {
                    Database = resourceName,
                    Exception = null,
                    FormattedMessage = formattedMessage,
                    Level = logLevel,
                    LoggerName = loggerName,
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
            logger.Log(logLevel, wrappedMessageFunc, exception);
            foreach (var target in targets)
            {
                target.Write(new LogEventInfo
                {
                    Exception = exception,
                    FormattedMessage = wrappedMessageFunc(),
                    Level = logLevel,
                    LoggerName = loggerName,
                    TimeStamp = SystemTime.UtcNow,
                });
            }
        }

        public bool ShouldLog(LogLevel logLevel)
        {
            return logger.ShouldLog(logLevel);
        }

        #endregion
    }
}
