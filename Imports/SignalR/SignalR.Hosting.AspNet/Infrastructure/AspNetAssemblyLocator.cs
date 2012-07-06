using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Web.Compilation;
using Raven.Imports.SignalR.Hubs;

namespace Raven.Imports.SignalR.Hosting.AspNet.Infrastructure
{
    public class AspNetAssemblyLocator : DefaultAssemblyLocator
    {
        public override IEnumerable<Assembly> GetAssemblies()
        {
            return BuildManager.GetReferencedAssemblies().Cast<Assembly>();
        }
    }
}