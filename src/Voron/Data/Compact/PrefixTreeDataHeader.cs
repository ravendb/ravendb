using Bond;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    [StructLayout(LayoutKind.Explicit, Size = 16, Pack = 1)]
    public struct PrefixTreeDataHeader
    {
        /// <summary>
        /// The back link to the prefix tree node that points to this data.
        /// </summary>
        [FieldOffset(0)]
        public long Origin;

        /// <summary>
        /// The size of the complete key for this element.
        /// </summary>
        [FieldOffset(8)]
        public int KeySize;

        /// <summary>
        /// The size of the data stored in this data node. 
        /// </summary>
        [FieldOffset(12)]
        public int DataSize;
    }
}
