// -----------------------------------------------------------------------
//  <copyright file="PerformanceRecord.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

namespace Performance.Comparison
{
	public class PerformanceRecord
	{
		public string Operation;
		public DateTime Time;
		public long Duration;
		public long ProcessedItems;
	}
}