using System;

namespace Sparrow.Logging;

internal sealed class RavenNullLogManager : IRavenLogManager
{
    public static readonly RavenNullLogManager Instance = new();

    private RavenNullLogManager()
    {
    }

    public IRavenLogger GetLogger(string name) => RavenNullLogger.Instance;

    public event EventHandler<RavenLoggingConfigurationChangedEventArgs> ConfigurationChanged = delegate { };

    public void Shutdown()
    {
    }

    private sealed class RavenNullLogger : IRavenLogger
    {
        public static readonly RavenNullLogger Instance = new();

        private RavenNullLogger()
        {
        }

        public bool IsErrorEnabled => false;
        public bool IsInfoEnabled => false;
        public bool IsDebugEnabled => false;
        public bool IsFatalEnabled => false;
        public bool IsTraceEnabled => false;
        public bool IsWarnEnabled => false;

        public void Error(string message)
        {
        }

        public void Error(string message, params object[] args)
        {
        }

        public void Error(string message, Exception exception)
        {
        }

        public void Error(Exception exception, string message, params object[] args)
        {
        }

        public void Error<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2)
        {
        }

        public void Error<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3)
        {
        }

        public void Info(string message)
        {
        }

        public void Info(string message, params object[] args)
        {
        }

        public void Info(string message, Exception exception)
        {
        }

        public void Info(Exception exception, string message, params object[] args)
        {
        }

        public void Info<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2)
        {
        }

        public void Info<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3)
        {
        }

        public void Debug(string message)
        {
        }

        public void Debug(string message, params object[] args)
        {
        }

        public void Debug(string message, Exception exception)
        {
        }

        public void Debug(Exception exception, string message, params object[] args)
        {
        }

        public void Debug<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2)
        {
        }

        public void Debug<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3)
        {
        }

        public void Warn(string message)
        {
        }

        public void Warn(string message, params object[] args)
        {
        }

        public void Warn(string message, Exception exception)
        {
        }

        public void Warn(Exception exception, string message, params object[] args)
        {
        }

        public void Warn<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2)
        {
        }

        public void Warn<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3)
        {
        }

        public void Fatal(string message)
        {
        }

        public void Fatal(string message, params object[] args)
        {
        }

        public void Fatal(string message, Exception exception)
        {
        }

        public void Fatal(Exception exception, string message, params object[] args)
        {
        }

        public void Fatal<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2)
        {
        }

        public void Fatal<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3)
        {
        }

        public void Trace(string message)
        {
        }

        public void Trace(string message, params object[] args)
        {
        }

        public void Trace(string message, Exception exception)
        {
        }

        public void Trace(Exception exception, string message, params object[] args)
        {
        }

        public void Trace<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2)
        {
        }

        public void Trace<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3)
        {
        }

        public bool IsEnabled(LogLevel logLevel) => false;

        public void Log(LogLevel logLevel, string message)
        {
        }

        public void Log(LogLevel logLevel, string message, Exception exception)
        {
        }

        public IRavenLogger WithProperty(string propertyKey, object propertyValue) => this;
    }
}
