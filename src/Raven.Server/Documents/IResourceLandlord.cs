using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Server.Json;

namespace Raven.Server.Documents
{
    public interface IResourceLandlord<TResource> : IDisposable
        where TResource : IResourceStore
    {
        Task<TResource> GetResourceInternal(string resourceName, RavenOperationContext context);

        bool TryGetOrCreateResourceStore(string resourceName, RavenOperationContext context, out Task<TResource> resourceTask);
    }
}