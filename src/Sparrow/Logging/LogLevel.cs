using System;
using NLog.Filters;

namespace Sparrow.Logging
{
    [Flags]
    public enum LogLevel
    {
        Trace = 0,
        Debug = 1,
        Info = 2,
        Warn = 3,
        Error = 4,
        Fatal = 5,
        Off = 6
    }

    public enum LogFilterAction
    {
        /// <summary>
        /// The filter doesn't want to decide whether to log or discard the message.
        /// </summary>
        Neutral,

        /// <summary>
        /// The message should be logged.
        /// </summary>
        Log,

        /// <summary>
        /// The message should not be logged.
        /// </summary>
        Ignore,

        /// <summary>
        /// The message should be logged and processing should be finished.
        /// </summary>
        LogFinal,

        /// <summary>
        /// The message should not be logged and processing should be finished.
        /// </summary>
        IgnoreFinal,
    }

    internal static class LogLevelExtensions
    {
        public static LogLevel FromNLogLogLevel(this NLog.LogLevel logLevel)
        {
            if (logLevel == NLog.LogLevel.Trace)
                return LogLevel.Trace;

            if (logLevel == NLog.LogLevel.Debug)
                return LogLevel.Debug;

            if (logLevel == NLog.LogLevel.Info)
                return LogLevel.Info;

            if (logLevel == NLog.LogLevel.Warn)
                return LogLevel.Warn;

            if (logLevel == NLog.LogLevel.Error)
                return LogLevel.Error;

            if (logLevel == NLog.LogLevel.Fatal)
                return LogLevel.Fatal;

            if (logLevel == NLog.LogLevel.Off)
                return LogLevel.Off;

            throw new ArgumentOutOfRangeException(nameof(logLevel), logLevel, null);
        }

        public static NLog.LogLevel ToNLogLogLevel(this LogLevel logLevel)
        {
            switch (logLevel)
            {
                case LogLevel.Trace:
                    return NLog.LogLevel.Trace;
                case LogLevel.Debug:
                    return NLog.LogLevel.Debug;
                case LogLevel.Info:
                    return NLog.LogLevel.Info;
                case LogLevel.Warn:
                    return NLog.LogLevel.Warn;
                case LogLevel.Error:
                    return NLog.LogLevel.Error;
                case LogLevel.Fatal:
                    return NLog.LogLevel.Fatal;
                case LogLevel.Off:
                    return NLog.LogLevel.Off;
                default:
                    throw new ArgumentOutOfRangeException(nameof(logLevel), logLevel, null);
            }
        }

        public static NLog.LogLevel ToNLogMaxLogLevel(this LogLevel logLevel)
        {
            switch (logLevel)
            {
                case LogLevel.Trace:
                case LogLevel.Debug:
                case LogLevel.Info:
                case LogLevel.Warn:
                case LogLevel.Error:
                case LogLevel.Fatal:
                    return NLog.LogLevel.Fatal;
                case LogLevel.Off:
                    return NLog.LogLevel.Off;
                default:
                    throw new ArgumentOutOfRangeException(nameof(logLevel), logLevel, null);
            }
        }

        public static NLog.LogLevel ToNLogFinalMinLogLevel(this LogLevel logLevel)
        {
            switch (logLevel)
            {
                case LogLevel.Trace:
                    return NLog.LogLevel.Off;
                case LogLevel.Debug:
                    return NLog.LogLevel.Trace;
                case LogLevel.Info:
                    return NLog.LogLevel.Debug;
                case LogLevel.Warn:
                    return NLog.LogLevel.Info;
                case LogLevel.Error:
                    return NLog.LogLevel.Warn;
                case LogLevel.Fatal:
                    return NLog.LogLevel.Error;
                case LogLevel.Off:
                    return NLog.LogLevel.Fatal;
                default:
                    throw new ArgumentOutOfRangeException(nameof(logLevel), logLevel, null);
            }
        }

        public static LogLevel FromNLogFinalMinLogLevel(this NLog.LogLevel logLevel)
        {
            if (logLevel == NLog.LogLevel.Trace)
                return LogLevel.Debug;

            if (logLevel == NLog.LogLevel.Debug)
                return LogLevel.Info;

            if (logLevel == NLog.LogLevel.Info)
                return LogLevel.Warn;

            if (logLevel == NLog.LogLevel.Warn)
                return LogLevel.Error;

            if (logLevel == NLog.LogLevel.Error)
                return LogLevel.Fatal;

            if (logLevel == NLog.LogLevel.Fatal)
                return LogLevel.Off;

            if (logLevel == NLog.LogLevel.Off)
                return LogLevel.Trace;

            throw new ArgumentOutOfRangeException(nameof(logLevel), logLevel, null);
        }
    }
}
