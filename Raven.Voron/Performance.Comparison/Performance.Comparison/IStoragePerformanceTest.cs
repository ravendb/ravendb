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

		List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data);
        List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data);
		PerformanceRecord ReadSequential();
		PerformanceRecord ReadRandom(IEnumerable<int> randomIds);
	}
}