using System.Collections.Generic;
using System.Reflection;

namespace Raven.Imports.SignalR.Hubs
{
    public interface IAssemblyLocator
    {
        IEnumerable<Assembly> GetAssemblies();
    }
}