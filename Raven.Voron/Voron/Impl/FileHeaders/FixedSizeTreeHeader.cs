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
			public ushort ValueSize;

			[FieldOffset(2)]
			public OptionFlags Flags;

			[FieldOffset(3)]
			public ushort NumberOfEntries;
		}

		[StructLayout(LayoutKind.Explicit, Pack = 1)]
		public struct Large
		{
			[FieldOffset(0)]
			public ushort ValueSize;

			[FieldOffset(2)]
			public OptionFlags Flags;

			[FieldOffset(3)]
			public long EntriesCount;

			[FieldOffset(11)]
			public long RootPageNumber;

            [FieldOffset(19)]
            public int Depth;

            [FieldOffset(23)]
            public long PageCount;
        }
	}
}