using System;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow.Json.Parsing;

namespace Sparrow.Logging;

public class SwitchLogger : Logger, IDisposable
{
    private LogAvailability _logAvailability; 
        
    internal readonly LoggersCollection Loggers;

    internal SwitchLogger(LoggingSource loggingSource, string name) : 
        base(loggingSource, null, name)
    {
        Loggers = new LoggersCollection(this);
        _logAvailability = loggingSource.LogAvailability;
    }
    
    internal SwitchLogger(SwitchLogger parent, string source, string name) : 
        base(parent, source, name)
    {
        Loggers = new LoggersCollection(this);
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

    internal Logger GetLogger<T>() => GetLogger(typeof(T).Name);
    internal Logger GetLogger(string name) => new Logger(this, $"{Source}.{name}", name);
    
    internal SwitchLogger GetSubSwitchLogger(string name)
    {
        if (string.IsNullOrEmpty(name))
            throw new InvalidOperationException($"{nameof(name)} {nameof(string.IsNullOrEmpty)}");

        return Loggers.GetOrAdd(name);
    }

    internal bool IsModeOverrode => _logAvailability != LoggingSource.LogAvailability;

    internal LogMode GetLogMode() => _logAvailability.GetMode();

    public void SetLoggerMode(LogMode mode)
    {
        _logAvailability = new LogAvailability(mode);
    }
    private void SetModeRecursively(LogAvailability logAvailability)
    {
        _logAvailability = logAvailability;
        foreach (var (_, child) in Loggers)
        {
            child.SetModeRecursively(logAvailability);
        }
    }

    public void Dispose()
    {
        Loggers.Dispose();
        Parent?.Loggers.TryRemove(Name);
    }

    public void ResetRecursively()
    {
        SetModeRecursively(LoggingSource.LogAvailability);
    }

    protected override LogMode? GetOverrideWriteMode()
    {
        return IsModeOverrode == false ? null : GetLogMode();
    }
    
    public DynamicJsonValue ToJson()
    {
        var ret = new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(Source)] = Source,
            
            [nameof(IsModeOverrode)] = IsModeOverrode,
            [nameof(LogMode)] = _logAvailability.GetMode(),
        };
        
        var loggers = Loggers.ToArray();
        if (loggers.Any())
        {
            var json = new DynamicJsonValue();
            foreach (var ( name, logger) in loggers)
            {
                json[name] = logger.ToJson();
            }
            ret[nameof(Loggers)] = json;
        }
        
        return ret;
    }}
