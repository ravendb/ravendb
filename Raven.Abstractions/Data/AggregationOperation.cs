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

		Distinct = 1 << 26,
		Dynamic = 1 << 27
	}
}