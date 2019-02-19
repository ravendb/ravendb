using System;
using System.Collections.Generic;
using System.Linq;
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
                Add(new Job
                {
                    Environment =
                    {
                        Runtime = Runtime.Core,
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
                Add(GetExporters().ToArray());
                // Generate plots using R if %R_HOME% is correctly set
                Add(RPlotExporter.Default);

                Add(BaselineValidator.FailOnError);
                Add(JitOptimizationsValidator.FailOnError);
                Add(EnvironmentAnalyser.Default);
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

        private class NumericClassComparer : IComparer<int>
        {
            public int Compare(int x, int y)
            {
                return x - y;
            }
        }

        [Benchmark(Baseline = true)]
        public void Framework()
        {
            var data = (int[])values.Clone();
            var comparer = new NumericClassComparer();
            Array.Sort(data, comparer);
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

    }
}
