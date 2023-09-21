using System;
using System.Collections.Concurrent;
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

namespace Micro.Benchmark.Benchmarks
{
    [Config(typeof(DictionaryComparerConfig))]
    public class DictionaryComparerBenchmark
    {
        private class DictionaryComparerConfig : ManualConfig
        {
            public DictionaryComparerConfig()
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

        private readonly List<int> _valuesToCheck = new();
        private readonly Dictionary<long, int> _longDefaultComparerDict = new(4096, EqualityComparer<long>.Default);
        private readonly Dictionary<long, int> _longComparerDict = new(4096, NumericEqualityComparer.BoxedInstanceInt64);
        private readonly Dictionary<long, int> _longNoComparerDict = new(4096);
        private readonly Dictionary<int, int> _integerDefaultComparerDict = new(4096, EqualityComparer<int>.Default);
        private readonly Dictionary<int, int> _integerComparerDict = new(4096, NumericEqualityComparer.BoxedInstanceInt32);
        private readonly Dictionary<int, int> _integerNoComparerDict = new(4096);

        private readonly ConcurrentDictionary<int, int> _integerNoComparerConcurrentDict = new();
        private readonly ConcurrentDictionary<int, int> _integerComparerConcurrentDict = new(NumericEqualityComparer.BoxedInstanceInt32);
        private readonly ConcurrentDictionary<int, int> _integerDefaultComparerConcurrentDict = new(EqualityComparer<int>.Default);

        private readonly HashSet<int> _integerDefaultComparerSet = new(4096, EqualityComparer<int>.Default);
        private readonly HashSet<int> _integerComparerSet = new(4096, NumericEqualityComparer.BoxedInstanceInt32);
        private readonly HashSet<int> _integerNoComparerSet = new(4096);


        [GlobalSetup]
        public void Setup()
        {
            var rnd = new Random();
            for (int i = 0; i < 2048; i++)
            {
                int value = rnd.Next();
                
                _valuesToCheck.Add(value);

                _longDefaultComparerDict[value] = value;
                _longComparerDict[value] = value;
                _longNoComparerDict[value] = value;

                _integerDefaultComparerDict[value] = value;
                _integerComparerDict[value] = value;
                _integerNoComparerDict[value] = value;

                _integerDefaultComparerConcurrentDict[value] = value;
                _integerComparerConcurrentDict[value] = value;
                _integerNoComparerConcurrentDict[value] = value;

                _integerDefaultComparerSet.Add(value);
                _integerComparerSet.Add(value);
                _integerNoComparerSet.Add(value);
            }
        }

        [Benchmark]
        public int DefaultComparerLong()
        {
            int r = 0;
            foreach (var value in _valuesToCheck)
                r += _longDefaultComparerDict[value];

            return r;
        }

        [Benchmark]
        public int NumericComparerLong()
        {
            int r = 0;
            foreach (var value in _valuesToCheck)
                r += _longComparerDict[value];

            return r;
        }

        [Benchmark]
        public int NakedComparerLong()
        {
            int r = 0;
            foreach (var value in _valuesToCheck)
                r += _longNoComparerDict[value];

            return r;
        }

        [Benchmark]
        public int DefaultComparerInteger()
        {
            int r = 0;
            foreach (var value in _valuesToCheck)
                r += _integerDefaultComparerDict[value];

            return r;
        }

        [Benchmark]
        public int NumericComparerInteger()
        {
            int r = 0;
            foreach (var value in _valuesToCheck)
                r += _integerComparerDict[value];

            return r;
        }

        [Benchmark(Baseline = true)]
        public int NakedComparerInteger()
        {
            int r = 0;
            foreach (var value in _valuesToCheck)
                r += _integerNoComparerDict[value];

            return r;
        }

        [Benchmark]
        public int ConcurrentDefaultComparerInteger()
        {
            int r = 0;
            foreach (var value in _valuesToCheck)
                r += _integerDefaultComparerConcurrentDict[value];

            return r;
        }

        [Benchmark]
        public int ConcurrentNumericComparerInteger()
        {
            int r = 0;
            foreach (var value in _valuesToCheck)
                r += _integerComparerConcurrentDict[value];

            return r;
        }

        [Benchmark]
        public int ConcurrentNakedComparerInteger()
        {
            int r = 0;
            foreach (var value in _valuesToCheck)
                r += _integerNoComparerConcurrentDict[value];

            return r;
        }

        [Benchmark]
        public int SetDefaultComparerInteger()
        {
            int r = 0;
            foreach (var value in _valuesToCheck)
            {
                if (_integerDefaultComparerSet.Contains(value))
                    r += value;
            }

            return r;
        }

        [Benchmark]
        public int SetNumericComparerInteger()
        {
            int r = 0;
            foreach (var value in _valuesToCheck)
            {
                if (_integerComparerSet.Contains(value))
                    r += value;
            }

            return r;
        }

        [Benchmark]
        public int SetNakedComparerInteger()
        {
            int r = 0;
            foreach (var value in _valuesToCheck)
            {
                if (_integerNoComparerSet.Contains(value))
                    r += value;
            }

            return r;
        }
    }

    internal readonly struct NumericEqualityComparer : IEqualityComparer<long>, IEqualityComparer<int>, IEqualityComparer<ulong>, IEqualityComparer<uint>
    {
        public static readonly IEqualityComparer<long> BoxedInstanceInt64 = new NumericEqualityComparer();
        public static readonly IEqualityComparer<int> BoxedInstanceInt32 = new NumericEqualityComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(long x, long y)
        {
            return x == y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(long obj)
        {
            return Hashing.Mix(obj);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(int x, int y)
        {
            return x == y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(int obj)
        {
            return Hashing.Mix(obj);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(ulong x, ulong y)
        {
            return x == y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(ulong obj)
        {
            return Hashing.Mix((long)obj);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(uint x, uint y)
        {
            return x == y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(uint obj)
        {
            return (int)Hashing.Mix(obj);
        }
    }
}
