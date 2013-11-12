// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackupInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;

namespace Voron.Impl.Backup
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct IncrementalBackupInfo
	{
		[FieldOffset(0)]
		public long LastBackedUpJournal;

		[FieldOffset(8)] 
		public long LastBackedUpJournalPage;

		[FieldOffset(16)]
		public long LastCreatedJournal;
	}
}