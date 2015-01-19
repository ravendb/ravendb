// -----------------------------------------------------------------------
//  <copyright file="CounterChanges.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Abstractions.Counters
{
	public class CounterChange
	{
		public string Name { get; set; }
		public string Group { get; set; }
		public long Delta { get; set; }
	}
}