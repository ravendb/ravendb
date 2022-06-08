using System;

namespace Raven.Server.Documents.Patch
{
    public interface IObjectInstance<T> : IDisposable
        where T : struct, IJsHandle<T>
    {
        IJsEngineHandle<T> EngineHandle { get; }

        T CreateJsHandle(bool keepAlive = false); // TODO [shlomo] may be eliminated with care
    }
}
