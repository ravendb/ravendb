//-----------------------------------------------------------------------
// <copyright file="AggregationOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
	[Flags]
	public enum AggregationOperation
	{
		None = 0,
		Count = 1,


		Dynamic = 0x8000000
	}
}