using System;

namespace Nevar.Impl
{
    [Flags]
    public enum FlushMode
    {
        None = 0,
        Buffers = 2,
        Full = 4
    }
}