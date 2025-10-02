using System;
using NLog;
using Sparrow.Logging;
using LogLevel = Sparrow.Logging.LogLevel;

namespace Sparrow.Server.Logging;

public sealed class RavenLogger : IRavenLogger
{
    private readonly Logger _logger;

    public RavenLogger(Logger logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public bool IsErrorEnabled => _logger.IsErrorEnabled;

    public bool IsInfoEnabled => _logger.IsInfoEnabled;

    public bool IsDebugEnabled => _logger.IsDebugEnabled;

    public bool IsFatalEnabled => _logger.IsFatalEnabled;

    public bool IsTraceEnabled => _logger.IsTraceEnabled;

    public bool IsWarnEnabled => _logger.IsWarnEnabled;

    public void Error(string message)
    {
        _logger.Error(message);
    }

    public void Error(string message, Exception exception)
    {
        _logger.Error(exception, message);
    }

    public void Error(Exception exception, string message, params object[] args)
    {
        _logger.Error(exception, message, args);
    }
    
    public void Error<TArgument>(string message, TArgument argument)
    {
        _logger.Error(message, argument);
    }
    
    public void Error<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2)
    {
        _logger.Error(message, argument1, argument2);
    }
    
    public void Error<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3)
    {
        _logger.Error(message, argument1, argument2, argument3);
    }

    public void Info(string message)
    {
        _logger.Info(message);
    }

    public void Info(string message, Exception exception)
    {
        _logger.Info(exception, message);
    }

    public void Info(Exception exception, string message, params object[] args)
    {
        _logger.Info(exception, message, args);
    }

    public void Info<TArgument>(string message, TArgument argument)
    {
        _logger.Info(message, argument);
    }

    public void Info<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2)
    {
        _logger.Info(message, argument1, argument2);
    }

    public void Info<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3)
    {
        _logger.Info(message, argument1, argument2, argument3);   
    }

    public void Debug(string message)
    {
        _logger.Debug(message);
    }

    public void Debug(string message, Exception exception)
    {
        _logger.Debug(exception, message);
    }

    public void Debug(Exception exception, string message, params object[] args)
    {
        _logger.Debug(exception, message, args);
    }

    public void Debug<TArgument>(string message, TArgument argument)
    {
        _logger.Debug(message, argument);
    }

    public void Debug<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2)
    {
        _logger.Debug(message, argument1, argument2);   
    }

    public void Debug<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3)
    {
        _logger.Debug(message, argument1, argument2, argument3);  
    }

    public void Warn(string message)
    {
        _logger.Warn(message);
    }

    public void Warn(string message, Exception exception)
    {
        _logger.Warn(exception, message);
    }

    public void Warn(Exception exception, string message, params object[] args)
    {
        _logger.Warn(exception, message, args);  
    }

    public void Warn<TArgument>(string message, TArgument argument)
    {
        _logger.Warn(message, argument); 
    }

    public void Warn<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2)
    {
        _logger.Warn(message, argument1, argument2);
    }

    public void Warn<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3)
    {
        _logger.Warn(message, argument1, argument2, argument3);
    }

    public void Fatal(string message)
    {
        _logger.Fatal(message);

        Console.Error.WriteLine(message);
    }

    public void Fatal(string message, Exception exception)
    {
        _logger.Fatal(exception, message);

        Console.Error.WriteLine(message + Environment.NewLine + exception);
    }

    public void Fatal(Exception exception, string message, params object[] args)
    {
        _logger.Fatal(exception, message, args);
    }

    public void Fatal<TArgument>(string message, TArgument argument)
    {
        _logger.Fatal(message, argument);
    }

    public void Fatal<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2)
    {
        _logger.Fatal(message, argument1, argument2);
    }

    public void Fatal<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3)
    {
        _logger.Fatal(message, argument1, argument2, argument3);   
    }

    public void Trace(string message)
    {
        _logger.Trace(message);
    }

    public void Trace(string message, Exception exception)
    {
        _logger.Trace(exception, message);
    }

    public void Trace(Exception exception, string message, params object[] args)
    {
        _logger.Trace(exception, message, args); 
    }

    public void Trace<TArgument>(string message, TArgument argument)
    {
        _logger.Trace(message, argument);
    }

    public void Trace<TArgument1, TArgument2>(string message, TArgument1 argument1, TArgument2 argument2)
    {
        _logger.Trace(message, argument1, argument2);
    }

    public void Trace<TArgument1, TArgument2, TArgument3>(string message, TArgument1 argument1, TArgument2 argument2, TArgument3 argument3)
    {
        _logger.Trace(message, argument1, argument2, argument3);
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        return _logger.IsEnabled(logLevel.ToNLogLogLevel());
    }

    public void Log(LogLevel logLevel, string message)
    {
        _logger.Log(logLevel.ToNLogLogLevel(), message);
    }

    public void Log(LogLevel logLevel, string message, Exception exception)
    {
        _logger.Log(logLevel.ToNLogLogLevel(), exception, message);
    }

    public IRavenLogger WithProperty(string propertyKey, object propertyValue)
    {
        return new RavenLogger(_logger.WithProperty(propertyKey, propertyValue));
    }
}
