using System;

namespace Voron.Impl.FileHeaders
{
    [Flags]
    public enum TreeFlags : byte
    {
        None = 0,
        MultiValue = 1,
        FixedSizeTrees = 2,
        MultiValueTrees = 4
    }
}
