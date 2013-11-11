// -----------------------------------------------------------------------
//  <copyright file="LogInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;

namespace Voron.Impl.Journal
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct LogInfo
	{
		[FieldOffset(0)]
		public long DataFlushCounter;

		[FieldOffset(8)]
		public long RecentLog;

		[FieldOffset(16)]
		public int LogFilesCount;

		[FieldOffset(24)]
		public long LastSyncedLog;

		[FieldOffset(32)]
		public long LastSyncedLogPage;
	}
}