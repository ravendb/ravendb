using System;
using Microsoft.Extensions.Logging;

namespace Raven.Server.Utils.MicrosoftLogging;

public sealed class MicrosoftLoggerFactory : ILoggerFactory
{
    private readonly MicrosoftLoggingProvider _provider;

    public MicrosoftLoggerFactory(MicrosoftLoggingProvider provider)
    {
        _provider = provider;
    }
    public void Dispose()
    {
    }

    public void AddProvider(ILoggerProvider provider)
    {
        //We don't need the complex logic of the build in LoggerProvider
        //This factory is just a bridge to the provider
        throw new InvalidOperationException("Should not add provider");
    }

    public ILogger CreateLogger(string categoryName)
    {
        return _provider.CreateLogger(categoryName);
    }
}
