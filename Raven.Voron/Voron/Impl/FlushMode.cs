using System;

namespace Voron.Impl
{
    [Flags]
    public enum FlushMode
    {
        None = 0,
        Buffers = 2,
        Full = 4
    }
}