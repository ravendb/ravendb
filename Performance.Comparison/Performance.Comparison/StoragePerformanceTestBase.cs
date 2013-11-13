namespace Performance.Comparison
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    public abstract class StoragePerformanceTestBase : IStoragePerformanceTest
    {
        private readonly byte[] _buffer;


        protected StoragePerformanceTestBase(byte[] buffer)
        {
            _buffer = buffer;
            GC.Collect();
        }

        public abstract string StorageName { get; }
        public virtual bool CanHandleBigData { get { return true; } }

        public abstract List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data, PerfTracker perfTracker);

        public abstract List<PerformanceRecord> WriteParallelSequential(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds);

        public abstract List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data, PerfTracker perfTracker);

        public abstract List<PerformanceRecord> WriteParallelRandom(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds);

        public abstract PerformanceRecord ReadSequential(PerfTracker perfTracker);

        public abstract PerformanceRecord ReadParallelSequential(PerfTracker perfTracker, int numberOfThreads);

        public abstract PerformanceRecord ReadRandom(IEnumerable<int> randomIds, PerfTracker perfTracker);

        public abstract PerformanceRecord ReadParallelRandom(IEnumerable<int> randomIds, PerfTracker perfTracker, int numberOfThreads);

        protected byte[] GetValueToWrite(byte[] currentValue, int newSize)
        {
            if (currentValue != null && currentValue.Length == newSize)
            {
                return currentValue;
            }

            currentValue = new byte[newSize];

            Array.Copy(_buffer, currentValue, newSize);

            return currentValue;
        }

        protected IList<ParallelTestData> SplitData(IEnumerable<TestData> data, int currentNumberOfTransactions, int currentNumberOfItemsPerTransaction, int numberOfThreads)
        {
            var count = data.Count();
            Debug.Assert(count == currentNumberOfItemsPerTransaction * currentNumberOfTransactions);

            var results = new List<ParallelTestData>();

            var numberOfTransactionsPerThread = currentNumberOfTransactions / numberOfThreads;

            for (var i = 0; i < numberOfThreads; i++)
            {
                var actualNumberOfTransactionsPerThread = i < numberOfThreads - 1
                                                              ? numberOfTransactionsPerThread
                                                              : currentNumberOfTransactions - (i * numberOfTransactionsPerThread);

                var item = new ParallelTestData
                               {
                                   Enumerator =
                                       data
                                       .Skip(i * currentNumberOfItemsPerTransaction * numberOfTransactionsPerThread)
                                       .Take(actualNumberOfTransactionsPerThread * currentNumberOfItemsPerTransaction)
                                       .GetEnumerator(),
                                   ItemsPerTransaction = currentNumberOfItemsPerTransaction,
                                   NumberOfTransactions = actualNumberOfTransactionsPerThread
                               };

                results.Add(item);
            }

            return results;
        }
    }
}