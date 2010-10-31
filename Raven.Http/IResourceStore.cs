using System;

namespace Raven.Http
{
    public interface IResourceStore : IDisposable
    {
        IRaveHttpnConfiguration Configuration { get; }
    }
}