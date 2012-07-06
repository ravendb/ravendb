using System;
using System.Collections.Generic;
using System.Reflection;

namespace Raven.Imports.SignalR.Hubs
{
    public class DefaultAssemblyLocator : IAssemblyLocator
    {
        public virtual IEnumerable<Assembly> GetAssemblies()
        {
            return AppDomain.CurrentDomain.GetAssemblies();
        }
    }
}