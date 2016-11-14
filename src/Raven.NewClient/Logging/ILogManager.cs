using System;

namespace Raven.NewClient.Abstractions.Logging
{
    public interface ILogManager
    {
        ILog GetLogger(string name);

        IDisposable OpenNestedConext(string message);

        IDisposable OpenMappedContext(string key, string value);
    }
}
