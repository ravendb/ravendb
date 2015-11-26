// -----------------------------------------------------------------------
//  <copyright file="FixedSizeTreeHeader.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;
using Voron.Impl.FileHeaders;

namespace Voron.Data.Fixed
{
	public class FixedSizeTreeHeader
	{
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