using System;
using Microsoft.Extensions.Logging;
using Sparrow.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

public class SparrowLoggerWrapper : ILogger<RavenServer>
{
    private readonly Logger _sparrowLogger;

    public LogLevel MinLogLevel { get; set; }

    public SparrowLoggerWrapper(Logger sparrowLogger)
    {
        _sparrowLogger = sparrowLogger;
    }

    public IDisposable BeginScope<TState>(TState state) where TState : notnull
    {
        return NullScope.Instance;
    }

    public bool IsEnabled(LogLevel logLevel) => logLevel >= MinLogLevel;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
    {
        if (formatter == null)
            throw new ArgumentNullException(nameof(formatter));
        
        var logLine = formatter(state, null);
        _sparrowLogger.Operations($"{logLevel}, {logLine}", exception);
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
