using System;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using Bench.Utils;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using Perfolizer.Horology;
using Perfolizer.Metrology;
using static Sparrow.Server.Utils.VxSort.BitonicSort;

namespace Bench
{
    class SmallSortConfig : ManualConfig
    {
        public SmallSortConfig()
        {
            SummaryStyle = new SummaryStyle(
                CultureInfo.InvariantCulture,
                true,
                SizeUnit.GB,
                TimeUnit.Nanosecond
            );
            AddJob(Job.LongRun);
            AddColumn(new TimePerNColumn());
        }
    }

    public class SmallSortBenchBase<T> where T : unmanaged, IComparable<T>
    {
        const ulong CACHELINE_SIZE = 64;
        protected virtual int InvocationsPerIteration { get; }

        protected int _iterationIndex = 0;
        T[] _originalValues;
        protected T[][] _arrays;
        GCHandle[] _gcHandles;
        protected unsafe T*[] _arrayPtrs;

        protected virtual int ArraySize { get; }

        protected unsafe T* _tmp;

        [GlobalSetup]
        public unsafe void Setup()
        {
            _tmp = (T*)Marshal.AllocHGlobal(sizeof(T) * 2 * ArraySize);
            var rolledUpArraySize = ArraySize + (int)(CACHELINE_SIZE / (ulong)sizeof(T));
            _originalValues = ValuesGenerator.ArrayOfUniqueValues<T>(rolledUpArraySize);
            _arrays = Enumerable
                .Range(0, InvocationsPerIteration)
                .Select(_ => new T[rolledUpArraySize])
                .ToArray();
            _gcHandles = _arrays.Select(a => GCHandle.Alloc(a, GCHandleType.Pinned)).ToArray();
            _arrayPtrs = new T*[InvocationsPerIteration];
            for (var i = 0; i < InvocationsPerIteration; i++)
            {
                var p = (T*)_gcHandles[i].AddrOfPinnedObject();
                if (((ulong)p) % CACHELINE_SIZE != 0)
                    p = (T*)((((ulong)p) + CACHELINE_SIZE) & ~(CACHELINE_SIZE - 1));

                _arrayPtrs[i] = p;
            }
        }

        [IterationCleanup]
        public void CleanupIteration() => _iterationIndex = 0; // after every iteration end we set the index to 0

        [IterationSetup]
        public void SetupArrayIteration() =>
            ValuesGenerator.FillArrays(ref _arrays, InvocationsPerIteration, _originalValues);
    }

    [GenericTypeArguments(typeof(int))] // value type
    [InvocationCount(InvocationsPerIterationValue)]
    [Config(typeof(SmallSortConfig))]
    public class SmallSortBench<T> : SmallSortBenchBase<T> where T : unmanaged, IComparable<T>
    {
        const int InvocationsPerIterationValue = 4096;
        protected override int InvocationsPerIteration => InvocationsPerIterationValue;
        protected override int ArraySize => N;

        [Params(8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88, 96, 104, 112, 120, 128)]
        public int N;

        [Benchmark(Baseline = true)]
        public unsafe void ArraySort() => Array.Sort(_arrays[_iterationIndex++]);

        [Benchmark]
        public unsafe void BitonicSort() => Sort((int*)_arrayPtrs[_iterationIndex++], N);
    }
}
