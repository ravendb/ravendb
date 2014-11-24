using System.Collections.Concurrent;

namespace Performance.Comparison
{
    using System.Collections.Generic;

    public class ParallelTestData
    {
        public ConcurrentQueue<TestData> Queue { get; set; }

        public IEnumerator<TestData> Enumerate()
        {
            TestData data;
            while (Queue.TryDequeue(out data))
            {
                yield return data;
            }
        }

        public long NumberOfTransactions { get; set; }

        public long ItemsPerTransaction { get; set; }
    }
}