using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Voron
{
    [Flags]
    public enum PageFlags : byte
    {
        Single = 1,
        Overflow = 2,
        VariableSizeTreePage = 4,
        FixedSizeTreePage = 8,
        ReservedValue2 = 16,
        RawData = 32,
        Compressed = 64,
        ReservedValue3 = 128,
    }
}
