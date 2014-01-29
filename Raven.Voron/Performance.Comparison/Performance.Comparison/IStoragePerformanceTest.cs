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
        List<PerformanceRecord> WriteParallelSequential(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds);
        List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data, PerfTracker perfTracker);
        List<PerformanceRecord> WriteParallelRandom(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds);

		PerformanceRecord ReadSequential(PerfTracker perfTracker);
        PerformanceRecord ReadParallelSequential(PerfTracker perfTracker, int numberOfThreads);
		PerformanceRecord ReadRandom(IEnumerable<uint> randomIds, PerfTracker perfTracker);
        PerformanceRecord ReadParallelRandom(IEnumerable<uint> randomIds, PerfTracker perfTracker, int numberOfThreads);
	}
}
