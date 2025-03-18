using System;

namespace Sparrow.Logging;

public interface IRavenLogger
{
    bool IsErrorEnabled { get; }
    bool IsInfoEnabled { get; }
    bool IsDebugEnabled { get; }
    bool IsFatalEnabled { get; }
    bool IsTraceEnabled { get; }
    bool IsWarnEnabled { get; }
    void Error(string message);
    void Error(string message, Exception exception);
    void Info(string message);
    void Info(string message, Exception exception);
    void Debug(string message);
    void Debug(string message, Exception exception);
    void Warn(string message);
    void Warn(string message, Exception exception);
    void Fatal(string message);
    void Fatal(string message, Exception exception);
    void Trace(string message);
    void Trace(string message, Exception exception);
    bool IsEnabled(LogLevel logLevel);
    void Log(LogLevel logLevel, string message);
    void Log(LogLevel logLevel, string message, Exception exception);
    IRavenLogger WithProperty(string propertyKey, object propertyValue);
}
