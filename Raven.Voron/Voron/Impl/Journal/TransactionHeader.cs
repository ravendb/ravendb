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
		public long PageNumberInLogFile;

		[FieldOffset(16)]
		public long TransactionId;

		[FieldOffset(24)]
		public long NextPageNumber;

		[FieldOffset(32)]
		public long LastPageNumber;

		[FieldOffset(40)]
		public int PageCount;

		[FieldOffset(44)]
		public int OverflowPageCount;

		[FieldOffset(48)]
		public uint Crc;

	    [FieldOffset(52)]
		public TreeRootHeader Root;

        [FieldOffset(114)]
        public TreeRootHeader FreeSpace;

        [FieldOffset(176)]
        public TransactionMarker TxMarker;
	}
}