namespace Sparrow.Logging;

public class LogAvailability
{
    public bool IsInfoEnabled;
    public bool IsOperationsEnabled;

    public LogAvailability()
    {
            
    }
        
    public LogAvailability(LogMode logMode)
    {
        InternalSetMode(logMode);
    }
        
    public LogMode GetMode()
    {
        return IsInfoEnabled
            ? LogMode.Information
            : IsOperationsEnabled
                ? LogMode.Operations
                : LogMode.None;
    }

    protected void InternalSetMode(LogMode logMode)
    {
        IsOperationsEnabled = (logMode & LogMode.Operations) == LogMode.Operations;
        IsInfoEnabled = (logMode & LogMode.Information) == LogMode.Information;
    }
}

internal class LoggingSourceLogAvailability : LogAvailability
{
    public void SetMode(LogMode logMode)
    {
        InternalSetMode(logMode);
    }
}

