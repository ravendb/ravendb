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

        bool CanHandleBigData { get;  }

		List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data, PerfTracker perfTracker);
        List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data, PerfTracker perfTracker);
		PerformanceRecord ReadSequential(PerfTracker perfTracker);
        PerformanceRecord ReadParallelSequential(PerfTracker perfTracker, int numberOfThreads);
		PerformanceRecord ReadRandom(IEnumerable<int> randomIds, PerfTracker perfTracker);
        PerformanceRecord ReadParallelRandom(IEnumerable<int> randomIds, PerfTracker perfTracker, int numberOfThreads);
	}
}