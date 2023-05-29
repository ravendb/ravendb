using Sparrow.Json.Parsing;

namespace Sparrow.Logging;

internal class LoggersHolder
{
    public LoggersHolder(LoggingSource loggingSource)
    {
        Generic = new SwitchLogger(loggingSource, "Generic");
        Memory = Generic.GetSubSwitchLogger("Memory");
    }

    public readonly SwitchLogger Generic;
    public readonly SwitchLogger Memory;
}
