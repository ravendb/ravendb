using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI;

public class InMemoryLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentQueue<LogEntry> _logs = new();

    public ILogger CreateLogger(string categoryName) => new InMemoryLogger(_logs);

    public IEnumerable<LogEntry> GetLogs() => _logs.ToArray();

    public void ClearLogs() => _logs.Clear();

    public void Dispose()
    {
        _logs.Clear();
    }

    private class InMemoryLogger : ILogger
    {
        private readonly ConcurrentQueue<LogEntry> _logs;

        public InMemoryLogger(ConcurrentQueue<LogEntry> logs)
        {
            _logs = logs;
        }

        public IDisposable BeginScope<TState>(TState state) => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception exception,
            Func<TState, Exception, string> formatter)
        {
            _logs.Enqueue(new LogEntry
            {
                LogLevel = logLevel,
                EventId = eventId,
                Message = formatter(state, exception),
                Exception = exception,
                Timestamp = DateTime.UtcNow
            });
        }
    }
}

public class LogEntry : IDynamicJsonValueConvertible
{
    public LogLevel LogLevel { get; set; }
    public EventId EventId { get; set; }
    public string Message { get; set; }
    public Exception Exception { get; set; }
    public DateTime Timestamp { get; set; }

    public DynamicJsonValue ToJson() =>
        new()
        {
            [nameof(LogLevel)] = LogLevel.ToString(),
            [nameof(EventId)] = EventId.ToString(),
            [nameof(Message)] = Message,
            [nameof(Exception)] = Exception?.ToString(),
            [nameof(Timestamp)] = Timestamp
        };
}
