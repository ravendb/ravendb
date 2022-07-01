using System;
using System.Runtime.InteropServices;

namespace Voron.Data.Fixed
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct FixedSizeTreeEntry<TVal>
        where TVal : unmanaged, IBinaryNumber<TVal> 
    {
        [FieldOffset(0)]
        public TVal Key;

        [FieldOffset(8)]
        public byte* Value;

        [FieldOffset(8)]
        public long PageNumber;
    }
}
