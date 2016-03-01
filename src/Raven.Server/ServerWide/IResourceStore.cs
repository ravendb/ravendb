using System;
using Raven.Server.Config;

namespace Raven.Server.ServerWide
{
    public interface IResourceStore : IDisposable
    {
        string Name { get; }

        string ResourceName { get; }

        RavenConfiguration Configuration { get; }
    }
}