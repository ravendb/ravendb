using System;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow.Json.Parsing;

namespace Sparrow.Logging;

public class SwitchLogger : Logger, IDisposable
{
    private LogAvailability _logAvailability; 
        
    public readonly LoggersCollection Loggers;

    public SwitchLogger(LoggingSource loggingSource, string name) : 
        base(loggingSource, null, name)
    {
        Loggers = new LoggersCollection(LoggingSource, this);
        _logAvailability = loggingSource.LogAvailability;
    }
    
    public SwitchLogger(SwitchLogger parent, string source, string name) : 
        base(parent, source, name)
    {
        Loggers = new LoggersCollection(LoggingSource, this);
        _logAvailability = parent._logAvailability;
    }

    public override bool IsInfoEnabled
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get { return _logAvailability.IsInfoEnabled; }
    }

    public override bool IsOperationsEnabled
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get { return _logAvailability.IsOperationsEnabled; }
    }

    public Logger GetLogger<T>() => GetLogger(typeof(T).Name);
    public Logger GetLogger(string name) => new Logger(this, $"{Source}.{name}", name);
    
    public SwitchLogger GetSubSwitchLogger(string name)
    {
        if (string.IsNullOrEmpty(name))
            throw new InvalidOperationException($"{nameof(name)} {nameof(string.IsNullOrEmpty)}");

        return Loggers.GetOrAdd(name);
    }

    public bool IsReset() => _logAvailability == LoggingSource.LogAvailability;

    public LogMode GetLogMode() => _logAvailability.GetMode();

    public void SetLoggerMode(LogMode mode) => _logAvailability = new LogAvailability(mode);

    public void Dispose()
    {
        Loggers.Dispose();
        Parent.Loggers.TryRemove(Name);
    }

    public void UpdateMode(LogMode mode)
    {
        _logAvailability = new LogAvailability
        {
            IsInfoEnabled = (mode & LogMode.Operations) == LogMode.Operations,
            IsOperationsEnabled = (mode & LogMode.Information) == LogMode.Information
        };
    }
    public void Reset(bool recursively)
    {
        _logAvailability = LoggingSource.LogAvailability;
        if (recursively)
        {
            foreach ((_, SwitchLogger switchLogger) in Loggers.ToArray())
            {
                switchLogger.Reset(true);
            }
        }
    }

    protected override LogMode? GetOverrideWriteMode()
    {
        return IsReset() == false ? GetLogMode() : null;
    }
    
    public DynamicJsonValue ToJson()
    {
        var ret = new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(Source)] = Source,
            
            [nameof(IsReset)] = IsReset(),
            [nameof(IsInfoEnabled)] = IsInfoEnabled,
            [nameof(IsOperationsEnabled)] = IsOperationsEnabled,
        };
        var loggers = Loggers.ToArray();
        if (loggers.Any())
        {
            var json = new DynamicJsonValue();
            foreach ((var name, var logger) in loggers)
            {
                json[name] = logger.ToJson();
            }
            ret[nameof(Loggers)] = json;
        }
        
        return ret;
    }
}
