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
		Start = 0x1,
		Split = 0x2,
		Commit = 0x4,
	}
}