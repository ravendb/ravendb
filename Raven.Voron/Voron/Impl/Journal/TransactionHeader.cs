// -----------------------------------------------------------------------
//  <copyright file="TransactionHeader.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;
using Voron.Impl.FileHeaders;

namespace Voron.Impl.Journal
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct TransactionHeader
	{
		[FieldOffset(0)]
		public ulong HeaderMarker;

		[FieldOffset(8)]
		public long TransactionId;

		[FieldOffset(16)]
		public long NextPageNumber;

		[FieldOffset(24)]
		public long LastPageNumber;

		[FieldOffset(32)]
		public int PageCount;

		[FieldOffset(36)]
		public int OverflowPageCount;

		[FieldOffset(40)]
		public uint Crc;

	    [FieldOffset(44)]
		public TreeRootHeader Root;

        [FieldOffset(106)]
        public TreeRootHeader FreeSpace;

        [FieldOffset(168)]
        public TransactionMarker TxMarker;
	}
}