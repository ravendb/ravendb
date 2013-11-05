// -----------------------------------------------------------------------
//  <copyright file="IStoragePerformanceTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;

namespace Performance.Comparison
{
	public interface IStoragePerformanceTest
	{
		string StorageName { get; }

		List<PerformanceRecord> WriteSequential();
		List<PerformanceRecord> WriteRandom(HashSet<int> randomIds);
		PerformanceRecord ReadSequential();
		PerformanceRecord ReadRandom(HashSet<int> randomIds);
	}
}