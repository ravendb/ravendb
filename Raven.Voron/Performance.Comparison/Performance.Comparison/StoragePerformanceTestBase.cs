namespace Performance.Comparison
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;

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

        protected List<PerformanceRecord> ExecuteWriteWithParallel(IEnumerable<TestData> data, int numberOfTransactions, int itemsPerTransaction, int numberOfThreads, Func<IEnumerator<TestData>, long, long, List<PerformanceRecord>> writeFunction, out long elapsedMilliseconds)
        {
            var countdownEvent = new CountdownEvent(numberOfThreads);

            var parallelData = SplitData(data, numberOfTransactions, itemsPerTransaction, numberOfThreads);

            var records = new List<PerformanceRecord>[numberOfThreads];
            var sw = Stopwatch.StartNew();

            for (int i = 0; i < numberOfThreads; i++)
            {
                ThreadPool.QueueUserWorkItem(
                    state =>
                    {
                        var index = (int)state;
                        var pData = parallelData[index];

                        records[index] = writeFunction(pData.Enumerator, pData.ItemsPerTransaction, pData.NumberOfTransactions);

                        countdownEvent.Signal();
                    },
                    i);
            }

            countdownEvent.Wait();
            sw.Stop();

            elapsedMilliseconds = sw.ElapsedMilliseconds;

            return records
                .SelectMany(x => x)
                .ToList();
        }

        protected PerformanceRecord ExecuteReadWithParallel(string operation, IEnumerable<int> ids, int numberOfThreads, Action readAction)
        {
            var countdownEvent = new CountdownEvent(numberOfThreads);

            var sw = Stopwatch.StartNew();

            for (int i = 0; i < numberOfThreads; i++)
            {
                ThreadPool.QueueUserWorkItem(
                    state =>
                    {
                        readAction();

                        countdownEvent.Signal();
                    });
            }

            countdownEvent.Wait();
            sw.Stop();

            return new PerformanceRecord
            {
                Operation = operation,
                Time = DateTime.Now,
                Duration = sw.ElapsedMilliseconds,
                ProcessedItems = ids.Count() * numberOfThreads
            };
        }

        private IList<ParallelTestData> SplitData(IEnumerable<TestData> data, int currentNumberOfTransactions, int currentNumberOfItemsPerTransaction, int numberOfThreads)
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