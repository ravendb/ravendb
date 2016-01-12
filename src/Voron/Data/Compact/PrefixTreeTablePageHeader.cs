using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.Compact
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct PrefixTreeTablePageHeader
    {
        [FieldOffset(0)]
        public long PageNumber;

        [FieldOffset(8)]
        public int OverflowSize;

        [FieldOffset(12)]
        public PageFlags Flags;

        /// <summary>
        /// The current capacity of the dictionary
        /// </summary>
        [FieldOffset(14)]
        public int Capacity;

        /// <summary>
        /// This is the real counter of how many items are in the hash-table (regardless of buckets)
        /// </summary>
        [FieldOffset(18)]
        public int Size;

        /// <summary>
        /// How many used buckets. 
        /// </summary>
        [FieldOffset(22)]
        public int NumberOfUsed;

        /// <summary>
        /// How many occupied buckets are marked deleted
        /// </summary>
        [FieldOffset(26)]
        public int NumberOfDeleted;

        /// <summary>
        /// The next growth threshold. 
        /// </summary>
        [FieldOffset(30)]
        public int NextGrowthThreshold;
    }
}
