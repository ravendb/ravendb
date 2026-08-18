using System.Security.Claims;

namespace Raven.Quill.Logging;

public sealed class QuillLogger<TCategory>(ILogger<TCategory> logger, QuillLogging logging)
{
    public bool AuditEnabled => logging.IsAuditEnabled;

    public void Audit(string action, string target, HttpContext? context, ClaimsPrincipal? principal = null) =>
        logging.Audit(action, target, context, principal);

    public bool IsDebugEnabled => logger.IsEnabled(LogLevel.Debug);

    public bool IsInfoEnabled => logger.IsEnabled(LogLevel.Information);

    public bool IsWarnEnabled => logger.IsEnabled(LogLevel.Warning);

    public bool IsErrorEnabled => logger.IsEnabled(LogLevel.Error);

    public bool IsEnabled(LogLevel logLevel) => logger.IsEnabled(logLevel);

    public void Log(LogLevel logLevel, string? message, params object?[] args) =>
        logger.Log(logLevel, message, args);

    public void Debug(string? message, params object?[] args) =>
        logger.LogDebug(message, args);

    public void Debug(Exception? exception, string? message, params object?[] args) =>
        logger.LogDebug(exception, message, args);

    public void Info(string? message, params object?[] args) =>
        logger.LogInformation(message, args);

    public void Info(Exception? exception, string? message, params object?[] args) =>
        logger.LogInformation(exception, message, args);

    public void Warn(string? message, params object?[] args) =>
        logger.LogWarning(message, args);

    public void Warn(Exception? exception, string? message, params object?[] args) =>
        logger.LogWarning(exception, message, args);

    public void Error(string? message, params object?[] args) =>
        logger.LogError(message, args);

    public void Error(Exception? exception, string? message, params object?[] args) =>
        logger.LogError(exception, message, args);
}
