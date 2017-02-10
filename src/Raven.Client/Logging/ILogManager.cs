using System;

namespace Raven.Client.Logging
{
    public interface ILogManager
    {
        ILog GetLogger(string name);

        IDisposable OpenNestedConext(string message);

        IDisposable OpenMappedContext(string key, string value);
    }
}
