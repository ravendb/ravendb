using System;

namespace Sparrow
{
    internal interface IDisposableQueryable
    {
        bool IsDisposed { get; }
    }
}
