using System;

namespace Raven.Client.Logging
{
    internal interface ILogManager
    {
        ILog GetLogger(string name);

        IDisposable OpenNestedConext(string message);

        IDisposable OpenMappedContext(string key, string value);
    }
}
