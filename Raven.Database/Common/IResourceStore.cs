using System;

using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Server.Connections;

namespace Raven.Database.Common
{
    public interface IResourceStore : IDisposable
    {
        string Name { get; }

        string ResourceName { get; }

        TransportState TransportState { get; }

        AtomicDictionary<object> ExtensionsState { get; }

        InMemoryRavenConfiguration Configuration { get; }
    }
}
