using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow;

namespace Micro.Benchmark.Benchmarks.Sorting
{
    [Config(typeof(Config))]
    public class SortingPerformance
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job
                {
                    Environment =
                    {
                        Runtime = CoreRuntime.Core70,
                        Platform = Platform.X64,
                        Jit = Jit.RyuJit,
                    },
                    Run =
                    {
                        // TODO: Next line is just for testing. Fine tune parameters.
                        //RunStrategy = RunStrategy.Monitoring,
                    }
                });

                // Exporters for data
                AddExporter(GetExporters().ToArray());
                // Generate plots using R if %R_HOME% is correctly set
                AddExporter(RPlotExporter.Default);

                AddValidator(BaselineValidator.FailOnError);
                AddValidator(JitOptimizationsValidator.FailOnError);

                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        [Params(8, 16, 64, 256, 1024, 1024 * 4, 1024 * 32, 1024 * 512)]
        public int ArraySize { get; set; }

        private static int[] GeneratorWithDuplicates(int size)
        {
            var gen = new Random(size);

            var result = new int[size];
            for (int i = 0; i < size; i++)
                result[i] = gen.Next(0, size / 4);
            return result;
        }

        private static int[] Generator(int size)
        {
            var gen = new Random(size);

            var result = new int[size];
            for (int i = 0; i < size; i++)
                result[i] = gen.Next();
            return result;
        }

        private int[] values;

        [GlobalSetup]
        public void Setup()
        {
            values = Generator(ArraySize);
        }

        [Benchmark]
        public void Baseline()
        {
            var data = (int[])values.Clone();
        }

        [Benchmark(Baseline = true)]
        public void Framework()
        {
            var data = (int[])values.Clone();
            Array.Sort(data, Comparer<int>.Default);
        }

        [Benchmark]
        public void Sparrow()
        {
            var data = (int[])values.Clone();
            var sorter = default(Sorter<int, NumericComparer>);
            sorter.Sort(data);
        }

        [Benchmark]
        public void JitIntrinsic()
        {
            var data = (int[])values.Clone();
            Array.Sort(data);
        }

        [Benchmark]
        public void MemoryExtensions()
        {
            var data = ((int[])values.Clone()).AsSpan();
            data.Sort();
        }

        internal readonly struct NumericComparer : IComparer<long>, IComparer<int>, IComparer<uint>, IComparer<ulong>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(long x, long y)
            {
                return Math.Sign(x - y);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(int x, int y)
            {
                return x - y;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(ulong x, ulong y)
            {
                // We need to use branching here because without sign flags we can overflow and return wrong values.
                return x == y ? 0 : x > y ? 1 : -1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(uint x, uint y)
            {
                // We need to use branching here because without sign flags we can overflow and return wrong values.
                return x == y ? 0 : x > y ? 1 : -1;
            }
        }
    }
}
