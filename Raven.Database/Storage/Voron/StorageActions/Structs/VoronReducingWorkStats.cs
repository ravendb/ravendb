// -----------------------------------------------------------------------
//  <copyright file="VoronReducingWorkStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Runtime.InteropServices;

namespace Raven.Database.Storage.Voron.StorageActions.Structs
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct VoronReducingWorkStats
	{
		[FieldOffset(0)]
		public int ReduceAttempts;

		[FieldOffset(4)]
		public int ReduceSuccesses;

		[FieldOffset(8)]
		public int ReduceErrors;

		[FieldOffset(12)]
		public VoronEtagStruct LastReducedEtag;

		[FieldOffset(28)]
		public long LastReducedTimestampTicks;
	}
}