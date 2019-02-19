// -----------------------------------------------------------------------
//  <copyright file="FixedSizeTreeHeader.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;

namespace Voron.Data.Fixed
{
    public class FixedSizeTreeHeader
    {
        /// <summary>
        /// The Embedded Fixed Size Tree Root Header.
        /// </summary>    
        /// <remarks>This header extends the <see cref="RootHeader"/> structure.</remarks>
        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        public struct Embedded
        {
            [FieldOffset(0)]
            public RootObjectType RootObjectType;

            [FieldOffset(2)]
            public ushort ValueSize;

            [FieldOffset(4)]
            public ushort NumberOfEntries;
        }

        /// <summary>
        /// The Large Fixed Size Tree Root Header.
        /// </summary>    
        /// <remarks>This header extends the <see cref="RootHeader"/> structure.</remarks>
        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        public struct Large
        {
            [FieldOffset(0)]
            public RootObjectType RootObjectType;

            [FieldOffset(2)]
            public ushort ValueSize;

            [FieldOffset(4)]
            public long NumberOfEntries;

            [FieldOffset(12)]
            public long RootPageNumber;

            [FieldOffset(20)]
            public int Depth;

            [FieldOffset(24)]
            public long PageCount;
        }
    }
}