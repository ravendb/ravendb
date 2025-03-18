using System;
using Sparrow.Logging;

namespace Sparrow.Server.Logging;

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
