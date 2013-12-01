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
		public long CurrentJournal;

		[FieldOffset(8)]
		public int JournalFilesCount;

		[FieldOffset(12)]
		public long LastSyncedJournal;

		[FieldOffset(20)]
		public long LastSyncedJournalPage;
	}
}