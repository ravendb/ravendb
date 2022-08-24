using Sparrow.Json.Parsing;

namespace Sparrow.Logging;

public class LoggersHolder
{
    private const string Root = null;

    private readonly SwitchLogger _rootLogger;
        
    public LoggersHolder(LoggingSource loggingSource)
    {
        _rootLogger = new SwitchLogger(loggingSource, Root);
        
        Generic = _rootLogger.GetSubSwitchLogger("Generic");
        Memory = Generic.GetSubSwitchLogger("Memory");
    }

    public readonly SwitchLogger Generic;
    public readonly SwitchLogger Memory;
    
    public DynamicJsonValue ToJson() => _rootLogger.ToJson();
}
