// -----------------------------------------------------------------------
//  <copyright file="VoronIndexingWorkStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.InteropServices;

namespace Raven.Database.Storage.Voron.StorageActions.Structs
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct VoronIndexingWorkStats
	{
		[FieldOffset(0)]
		public int IndexingAttempts;

		[FieldOffset(4)]
		public int IndexingSuccesses;

		[FieldOffset(8)]
		public int IndexingErrors;

		[FieldOffset(12)]
		public long LastIndexingTimeTicks;

		[FieldOffset(20)]
		public int IndexId;

		[FieldOffset(24)]
		public long CreatedTimestampTicks;
	}
}