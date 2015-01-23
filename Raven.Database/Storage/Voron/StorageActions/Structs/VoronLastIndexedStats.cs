// -----------------------------------------------------------------------
//  <copyright file="VoronLastIndexedStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Runtime.InteropServices;

namespace Raven.Database.Storage.Voron.StorageActions.Structs
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct VoronLastIndexedStats
	{
		[FieldOffset(0)]
		public int IndexId;

		[FieldOffset(4)]
		public VoronEtagStruct LastEtag;

		[FieldOffset(20)]
		public long LastTimestampTicks;
	}
}