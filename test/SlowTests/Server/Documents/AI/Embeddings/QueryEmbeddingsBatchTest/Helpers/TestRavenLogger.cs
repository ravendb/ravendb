using System;
using System.Collections.Generic;
using Sparrow.Logging;

namespace SlowTests.Server.Documents.AI.Embeddings.QueryEmbeddingsBatchTest.Helpers;

public class TestRavenLogger(List<string> logEntries) : IRavenLogger
{
    public List<string> LogEntries { get; } = logEntries;

    public IRavenLogger WithProperty(string propertyKey, object propertyValue) => this;
    public bool IsTraceEnabled { get; set; } = true;
    public bool IsDebugEnabled { get; set; } = true;
    public bool IsInfoEnabled { get; set; } = true;
    public bool IsWarnEnabled { get; set; } = true;
    public bool IsErrorEnabled { get; set; } = true;
    public bool IsFatalEnabled { get; set; } = true;

    public bool IsEnabled(LogLevel logLevel) => logLevel switch
    {
        LogLevel.Trace => IsTraceEnabled,
        LogLevel.Debug => IsDebugEnabled,
        LogLevel.Info => IsInfoEnabled,
        LogLevel.Warn => IsWarnEnabled,
        LogLevel.Error => IsErrorEnabled,
        LogLevel.Fatal => IsFatalEnabled,
        LogLevel.Off => (IsTraceEnabled || IsDebugEnabled || IsInfoEnabled|| IsWarnEnabled || IsErrorEnabled || IsFatalEnabled) == false,
        _ => throw new ArgumentOutOfRangeException(nameof(logLevel), logLevel, null)
    };

    public void Debug(string message) => LogEntries.Add($"DEBUG: {message}");
    public void Debug(string message, Exception exception) => LogEntries.Add($"DEBUG: {message} - {exception.Message}");

    public void Info(string message) => LogEntries.Add($"INFO: {message}");
    public void Info(string message, Exception exception) => LogEntries.Add($"INFO: {message} - {exception.Message}");

    public void Warn(string message) => LogEntries.Add($"WARN: {message}");
    public void Warn(string message, Exception ex) => LogEntries.Add($"WARN: {message} - {ex.Message}");

    public void Error(string message) => LogEntries.Add($"ERROR: {message}");
    public void Error(string message, Exception ex) => LogEntries.Add($"ERROR: {message} - {ex.Message}");

    public void Fatal(string message) => LogEntries.Add($"FATAL: {message}");
    public void Fatal(string message, Exception exception) => LogEntries.Add($"FATAL: {message} - {exception.Message}");

    public void Trace(string message) => LogEntries.Add($"TRACE: {message}");
    public void Trace(string message, Exception exception) => LogEntries.Add($"TRACE: {message} - {exception.Message}");

    public void Log(LogLevel logLevel, string message) => LogEntries.Add($"{logLevel}: {message}");
    public void Log(LogLevel logLevel, string message, Exception exception) => LogEntries.Add($"{logLevel}: {message} - {exception.Message}");
}
