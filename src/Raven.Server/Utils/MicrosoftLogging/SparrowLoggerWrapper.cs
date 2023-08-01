using System;
using Microsoft.Extensions.Logging;
using Sparrow.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

public sealed class SparrowLoggerWrapper : ILogger<RavenServer>
{
    private readonly Logger _sparrowLogger;

    public LogLevel MinLogLevel { get; set; }

    public SparrowLoggerWrapper(Logger sparrowLogger, LogLevel logLevel)
    {
        _sparrowLogger = sparrowLogger;
        MinLogLevel = logLevel;
    }

    public IDisposable BeginScope<TState>(TState state) where TState : notnull
    {
        return NullScope.Instance;
    }

    public bool IsEnabled(LogLevel logLevel) => _sparrowLogger.IsOperationsEnabled && logLevel >= MinLogLevel;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
    {
        if (IsEnabled(logLevel) == false)
            return;
        
        if (formatter == null)
            throw new ArgumentNullException(nameof(formatter));
        
        var logLine = formatter(state, null);
        _sparrowLogger.Operations($"{logLevel.ToStringWithoutBoxing()}, {logLine}", exception);
    }
    
    private sealed class NullScope : IDisposable
    {
        public static NullScope Instance { get; } = new NullScope();

        private NullScope()
        {
        }

        public void Dispose()
        {
            // Nothing to do
        }
    }
}
