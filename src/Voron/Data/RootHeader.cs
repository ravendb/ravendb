using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Voron.Data
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct RootHeader
    {
        [FieldOffset(0)]
        public RootObjectType RootObjectType;
    }
}
