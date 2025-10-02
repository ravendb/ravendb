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
    void Error(Exception exception, string message, params object[] args);
    void Error<TArgument>(string message, TArgument argument);
    void Error<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2);
    void Error<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3);
    void Info(string message);
    void Info(string message, Exception exception);
    void Info(Exception exception, string message, params object[] args);
    void Info<TArgument>(string message, TArgument argument);
    void Info<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2);
    void Info<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3);
    void Debug(string message);
    void Debug(string message, Exception exception);
    void Debug(Exception exception, string message, params object[] args);
    void Debug<TArgument>(string message, TArgument argument);
    void Debug<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2);
    void Debug<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3);
    void Warn(string message);
    void Warn(string message, Exception exception);
    void Warn(Exception exception, string message, params object[] args);
    void Warn<TArgument>(string message, TArgument argument);
    void Warn<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2);
    void Warn<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3);
    void Fatal(string message);
    void Fatal(string message, Exception exception);
    void Fatal(Exception exception, string message, params object[] args);
    void Fatal<TArgument>(string message, TArgument argument);
    void Fatal<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2);
    void Fatal<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3);
    void Trace(string message);
    void Trace(string message, Exception exception);
    void Trace(Exception exception, string message, params object[] args);
    void Trace<TArgument>(string message, TArgument argument);
    void Trace<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2);
    void Trace<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3);
    bool IsEnabled(LogLevel logLevel);
    void Log(LogLevel logLevel, string message);
    void Log(LogLevel logLevel, string message, Exception exception);
    IRavenLogger WithProperty(string propertyKey, object propertyValue);
}
