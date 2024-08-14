using System;
using System.Globalization;
using System.Net.Http.Headers;
using Bench.Utils;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using BenchmarkDotNet.Extensions;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using Perfolizer.Horology;
using Perfolizer.Metrology;
using Sparrow.Server.Utils.VxSort;

namespace Bench
{
    class LongConfig : ManualConfig
    {
        public LongConfig()
        {
            SummaryStyle = new SummaryStyle(
                CultureInfo.InvariantCulture,
                true,
                SizeUnit.GB,
                TimeUnit.Microsecond
            );
            AddJob(Job.LongRun);
        }
    }

    class MediumConfig : ManualConfig
    {
        public MediumConfig()
        {
            SummaryStyle = new SummaryStyle(
                CultureInfo.InvariantCulture,
                true,
                SizeUnit.GB,
                TimeUnit.Microsecond
            );
            AddJob(Job.MediumRun);
        }
    }

    class ShortConfig : ManualConfig
    {
        public ShortConfig()
        {
            SummaryStyle = new SummaryStyle(
                CultureInfo.InvariantCulture,
                true,
                SizeUnit.GB,
                TimeUnit.Microsecond
            );
            AddJob(Job.ShortRun);
        }
    }

    public class SortBenchBase<T> where T : unmanaged, IComparable<T>
    {
        protected virtual int InvocationsPerIteration { get; }
        protected int _iterationIndex = 0;
        T[] _values;
        protected T[][] _arrays;

        [Params(10, 100, 1_000, 10_000, 100_000, 1_000_000)] //, 10_000_000)]
        public int N;

        [GlobalSetup]
        public void Setup() => _values = ValuesGenerator.ArrayOfUniqueValues<T>(N);

        [IterationCleanup]
        public void CleanupIteration() => _iterationIndex = 0; // after every iteration end we set the index to 0

        [IterationSetup]
        public void SetupArrayIteration() => ValuesGenerator.FillArrays(ref _arrays, InvocationsPerIteration, _values);
    }

    [GenericTypeArguments(typeof(int))] // value type
    [InvocationCount(InvocationsPerIterationValue)]
    [Config(typeof(MediumConfig))]
    [InliningDiagnoser(true, allowedNamespaces: new[] { "Sparrow.Server.Utils.VxSort" })]
    public class IntSort<T> : SortBenchBase<T> where T : unmanaged, IComparable<T>
    {
        const int InvocationsPerIterationValue = 10;

        protected override int InvocationsPerIteration => InvocationsPerIterationValue;

        [Benchmark(Baseline = true)]
        public void ArraySort() => Array.Sort(_arrays[_iterationIndex++]);

        [Benchmark]
        public void MemoryExtensions() => System.MemoryExtensions.Sort(_arrays[_iterationIndex++].AsSpan());

        [Benchmark]
        public void VxSort() => Sort.Run(_arrays[_iterationIndex++]);
    }

    [GenericTypeArguments(typeof(long))] // value type
    [InvocationCount(InvocationsPerIterationValue)]
    [Config(typeof(LongConfig))]
    public class LongSortBench<T> : SortBenchBase<T> where T : unmanaged, IComparable<T>
    {
        const int InvocationsPerIterationValue = 10;

        protected override int InvocationsPerIteration => InvocationsPerIterationValue;

        [Benchmark(Baseline = true)]
        public void Reference() => Array.Sort(_arrays[_iterationIndex++]);

        [Benchmark]
        public void MemoryExtensions() => System.MemoryExtensions.Sort(_arrays[_iterationIndex++].AsSpan());

        [Benchmark]
        public void VxSort() => Sort.Run(_arrays[_iterationIndex++]);
    }
}
