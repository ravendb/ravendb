// -----------------------------------------------------------------------
//  <copyright file="JournalInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;

namespace Voron.Impl.Journal
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct JournalInfo
	{
		[FieldOffset(0)]
		public long DataFlushCounter;

		[FieldOffset(8)]
		public long CurrentJournal;

		[FieldOffset(16)]
		public int JournalFilesCount;

		[FieldOffset(24)]
		public long LastSyncedJournal;

		[FieldOffset(32)]
		public long LastSyncedJournalPage;
	}
}