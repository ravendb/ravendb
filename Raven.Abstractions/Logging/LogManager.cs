using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging.LogProviders;
using Sparrow.Collections;

namespace Raven.Abstractions.Logging
{
    public static class LogManager
    {
        private static readonly ConcurrentSet<Target> Targets = new ConcurrentSet<Target>();

        public static void EnsureValidLogger()
        {
            GetLogger(typeof(LogManager));
        }

#if !DNXCORE50
        public static ILog GetCurrentClassLogger()
        {
            var stackFrame = new StackFrame(1, false);
            return GetLogger(stackFrame.GetMethod().DeclaringType);
        }
#endif

        private static ILogManager currentLogManager;
        public static ILogManager CurrentLogManager
        {
            get { return currentLogManager ?? (currentLogManager = ResolveExternalLogManager()); }
            set { currentLogManager = value; }
        }

        public static ILog GetLogger(Type type)
        {
            return GetLogger(type.FullName);
        }

        public static bool EnableDebugLogForTargets { get; set; }

        public static ILog GetLogger(string name)
        {
            ILogManager logManager = CurrentLogManager;
            if (logManager == null)
                return new LoggerExecutionWrapper(new NoOpLogger(), name, Targets);

            // This can throw in a case of invalid NLog.config file.
            var log = logManager.GetLogger(name);

            if (log  == null)
                return new LoggerExecutionWrapper(new NoOpLogger(), name, Targets);

            return new LoggerExecutionWrapper(log, name, Targets);
        }

        private static ILogManager ResolveExternalLogManager()
        {
            if (NLogLogManager.IsLoggerAvailable())
            {
                return new NLogLogManager();
            }
            if (Log4NetLogManager.IsLoggerAvailable())
            {
                return new Log4NetLogManager();
            }
            return null;
        }

        public static void RegisterTarget<T>() where T : Target, new()
        {
            if (Targets.OfType<T>().Any())
                return;

            Targets.Add(new T());
        }

        public static T GetTarget<T>() where T : Target
        {
            return Targets.OfType<T>().FirstOrDefault();
        }

        public class NoOpLogger : ILog
        {
            public bool IsInfoEnabled { get { return false; } }

            public bool IsDebugEnabled { get { return false; } }

            public bool IsWarnEnabled { get { return false; } }

            public void Log(LogLevel logLevel, Func<string> messageFunc)
            { }

            public void Log<TException>(LogLevel logLevel, Func<string> messageFunc, TException exception)
                where TException : Exception
            { }

            public bool ShouldLog(LogLevel logLevel)
            {
                return false;
            }
        }

        public static IDisposable OpenNestedConext(string context)
        {
            ILogManager logManager = CurrentLogManager;
            return logManager == null ? new DisposableAction(() => { }) : logManager.OpenNestedConext(context);
        }

        public static IDisposable OpenMappedContext(string key, string value)
        {
            ILogManager logManager = CurrentLogManager;
            return logManager == null ? new DisposableAction(() => { }) : logManager.OpenMappedContext(key, value);
        }

        public static void ClearTargets()
        {
            Targets.Clear();
        }

        public static bool ShouldLogToTargets(LogLevel logLevel, ILog logger)
        {
            switch (logLevel)
            {
                case LogLevel.Debug:
                case LogLevel.Info:
                    return logger.IsDebugEnabled;
                case LogLevel.Warn:
                    return logger.IsWarnEnabled;
                case LogLevel.Error:
                case LogLevel.Fatal:
                    return true; // errors & fatal are ALWAYS logged to registered targets
                default:
                    return true;
            }
        }
    }

    public abstract class Target : IDisposable
    {
        public abstract void Write(LogEventInfo logEvent);

        public abstract Boolean ShouldLog(ILog logger, LogLevel level);

        public virtual void Dispose()
        {
        }
    }

    public class LogEventInfo
    {
        public string Database { get; set; }
        public LogLevel Level { get; set; }
        public DateTime TimeStamp { get; set; }
        public string FormattedMessage { get; set; }
        public string LoggerName { get; set; }
        public Exception Exception { get; set; }
#if !DNXCORE50
        public StackTrace StackTrace { get; set; }
#endif
    }

    public class LogEventInfoFormatted
    {
        public String Level { get; set; }
        public string Database { get; set; }
        public DateTime TimeStamp { get; set; }
        public string Message { get; set; }
        public string LoggerName { get; set; }
        public string Exception { get; set; }
#if !DNXCORE50
        public string StackTrace { get; set; }
#endif

        public LogEventInfoFormatted(LogEventInfo eventInfo)
        {
            TimeStamp = eventInfo.TimeStamp;
            Message = eventInfo.FormattedMessage;
            LoggerName = eventInfo.LoggerName;
            Level = eventInfo.Level.ToString();
            Exception = eventInfo.Exception == null ? null : eventInfo.Exception.ToString();
            Database = eventInfo.Database;
#if !DNXCORE50
            StackTrace = eventInfo.StackTrace == null ? null : eventInfo.StackTrace.ToString();
#endif
        }
    }
}
