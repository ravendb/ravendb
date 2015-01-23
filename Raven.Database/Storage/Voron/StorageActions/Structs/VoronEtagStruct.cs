// -----------------------------------------------------------------------
//  <copyright file="VoronEtagStruct.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Runtime.InteropServices;
using Raven.Abstractions.Data;

namespace Raven.Database.Storage.Voron.StorageActions.Structs
{
	[StructLayout(LayoutKind.Explicit, Pack = 1)]
	public struct VoronEtagStruct
	{
		[FieldOffset(0)]
		public long Restarts;

		[FieldOffset(8)]
		public long Changes;

		public VoronEtagStruct(Etag etag)
		{
			Restarts = etag.Restarts;
			Changes = etag.Changes;
		}

		public Etag ToEtag()
		{
			return new Etag(0, Restarts, Changes); // no need to specify type explicitly
		}
	}
}