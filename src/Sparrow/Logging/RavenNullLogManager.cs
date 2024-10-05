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

        public void Error(string message, Exception exception)
        {
        }

        public void Info(string message)
        {
        }

        public void Info(string message, Exception exception)
        {
        }

        public void Debug(string message)
        {
        }

        public void Debug(string message, Exception exception)
        {
        }

        public void Warn(string message)
        {
        }

        public void Warn(string message, Exception exception)
        {
        }

        public void Fatal(string message)
        {
        }

        public void Fatal(string message, Exception exception)
        {
        }

        public void Trace(string message)
        {
        }

        public void Trace(string message, Exception exception)
        {
        }

        public bool IsEnabled(LogLevel logLevel) => false;

        public void Log(LogLevel logLevel, string message)
        {
        }

        public IRavenLogger WithProperty(string propertyKey, object propertyValue) => this;
    }
}
