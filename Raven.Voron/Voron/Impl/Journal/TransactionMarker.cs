// -----------------------------------------------------------------------
//  <copyright file="TransactionMarker.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Voron.Impl.Journal
{
	[Flags]
	public enum TransactionMarker : byte
	{
		None = 0x0,
		Commit = 0x4,
	}
}