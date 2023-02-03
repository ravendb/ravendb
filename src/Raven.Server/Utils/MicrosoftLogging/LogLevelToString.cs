using Microsoft.Extensions.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

public static class LogLevelToString
{
    private const string Trace          = nameof(LogLevel.Trace);
    private const string Debug          = nameof(LogLevel.Debug);
    private const string Information    = nameof(LogLevel.Information);
    private const string Warning        = nameof(LogLevel.Warning);
    private const string Error          = nameof(LogLevel.Error);
    private const string Critical       = nameof(LogLevel.Critical);
    private const string None           = nameof(LogLevel.None);
                                            
    public static string ToStringWithoutBoxing(this LogLevel logLevel)
    {
        return logLevel switch
        {
            LogLevel.Trace          => Trace,
            LogLevel.Debug          => Debug,
            LogLevel.Information    => Information,
            LogLevel.Warning        => Warning,
            LogLevel.Error          => Error,
            LogLevel.Critical       => Critical,
            LogLevel.None           => None,
            _ => logLevel.ToString()
        };                          
    }
}
