using Microsoft.Extensions.Logging;
using Sparrow.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

[ProviderAlias("Sparrow")]
public class SparrowLoggingProvider : ILoggerProvider
{
    private readonly LoggingSource _loggingSource;

    public SparrowLoggingProvider(LoggingSource loggingSource)
    {
        _loggingSource = loggingSource;
    }

    public ILogger CreateLogger(string categoryName)
    {
        var lastDot = categoryName.LastIndexOf('.');
        (string source, string logger) = lastDot >= 0
            ? (categoryName.Substring(0, lastDot), categoryName.Substring(lastDot + 1, categoryName.Length - lastDot - 1))
            : (categoryName, categoryName);
        var sparrowLogger = _loggingSource.GetLogger(source, logger);
        return new SparrowLoggerWrapper<RavenServer>(sparrowLogger);
    }
    
    public void Dispose()
    {
    }
}
