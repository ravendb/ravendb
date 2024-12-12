using System;

namespace Sparrow
{
    public interface IDisposableQueryable
    {
        bool IsDisposed { get; }
    }
}
