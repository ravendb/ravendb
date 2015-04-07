using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Server.Connections;

namespace Raven.Database.Server.Abstractions
{
    public interface IResourceStore
    {
        string Name { get; }

		string ResourceName { get; }

        TransportState TransportState { get; }

		AtomicDictionary<object> ExtensionsState { get; }

		InMemoryRavenConfiguration Configuration { get; }
    }
}
