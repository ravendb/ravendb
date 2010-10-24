using System;
using Raven.Database.Server.Responders;

namespace Raven.Http
{
    public interface IResourceStore : IDisposable
    {
        IRaveHttpnConfiguration Configuration { get; }
    }
}