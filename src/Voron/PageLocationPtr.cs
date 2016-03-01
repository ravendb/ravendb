using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Voron
{
    [StructLayout(LayoutKind.Explicit, Size = 16, Pack = 1)]
    public struct PageLocationPtr
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public long Offset;

        public bool IsValid
        {
            get { return PageNumber != -1; }
        }
    }
}
