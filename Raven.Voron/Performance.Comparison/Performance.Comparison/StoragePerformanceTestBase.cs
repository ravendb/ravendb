namespace Performance.Comparison
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public abstract class StoragePerformanceTestBase : IStoragePerformanceTest
    {
        private readonly byte[] _buffer;

        private readonly Process _process;

        protected StoragePerformanceTestBase(byte[] buffer)
        {
            _buffer = buffer;
            _process = Process.GetCurrentProcess();
            GC.Collect();
        }

        public abstract string StorageName { get; }

        public abstract List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data);

        public abstract List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data);

        public abstract PerformanceRecord ReadSequential();

        public abstract PerformanceRecord ReadRandom(IEnumerable<int> randomIds);

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

        protected long GetMemory()
        {
            _process.Refresh();

            return _process.PrivateMemorySize64;
        }
    }
}