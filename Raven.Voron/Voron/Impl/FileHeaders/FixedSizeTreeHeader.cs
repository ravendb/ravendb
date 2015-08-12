// -----------------------------------------------------------------------
//  <copyright file="FixedSizeTreeHeader.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;

namespace Voron.Impl.FileHeaders
{
	public class FixedSizeTreeHeader
	{
		public enum OptionFlags : byte
		{
			Embedded = 1,
			Large = 2
		}

		[StructLayout(LayoutKind.Explicit, Pack = 1)]
		public struct Embedded
		{
			[FieldOffset(0)]
			public byte ValueSize;

			[FieldOffset(1)]
			public OptionFlags Flags;

			[FieldOffset(2)]
			public byte NumberOfEntries;
		}

		[StructLayout(LayoutKind.Explicit, Pack = 1)]
		public struct Large
		{
			[FieldOffset(0)]
			public byte ValueSize;

			[FieldOffset(1)]
			public OptionFlags Flags;

			[FieldOffset(2)]
			public long NumberOfEntries;

			[FieldOffset(10)]
			public long RootPageNumber;

            [FieldOffset(18)]
            public int Depth;
        }
	}
}