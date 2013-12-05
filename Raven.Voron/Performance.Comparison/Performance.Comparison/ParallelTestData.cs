namespace Performance.Comparison
{
    using System.Collections.Generic;

    public class ParallelTestData
    {
        public IEnumerator<TestData> Enumerator { get; set; }

        public long NumberOfTransactions { get; set; }

        public long ItemsPerTransaction { get; set; }
    }
}