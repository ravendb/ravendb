using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow.Server;

namespace Micro.Benchmark
{
    [Config(typeof(Config))]
    public class Allocators
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

        public const int OperationsPerRound = 100000;

        private static readonly BlockAllocator<PoolAllocator<PoolAllocator.Default>> _pooAllocator;
        private static readonly FixedSizeAllocator<FixedSizePoolAllocator<FixedSizePoolAllocator.Default>> _fixedSizePoolAllocator;
        private static readonly Allocator<NativeAllocator<NativeAllocator.Default>> _nativeAllocator;
        private static readonly Allocator<ArenaAllocator<ArenaAllocator.Default>> _arenaAllocator;
        private static readonly Allocator<FragmentAllocator<FragmentAllocator.Default>> _fragmentAllocator;

        static Allocators()
        {
            _nativeAllocator = new Allocator<NativeAllocator<NativeAllocator.Default>>();
            _nativeAllocator.Initialize(default(NativeAllocator.Default));

            _pooAllocator = new BlockAllocator<PoolAllocator<PoolAllocator.Default>>();
            _pooAllocator.Initialize(default(PoolAllocator.Default));

            _arenaAllocator = new Allocator<ArenaAllocator<ArenaAllocator.Default>>();
            _arenaAllocator.Initialize(default(ArenaAllocator.Default));

            _fragmentAllocator = new Allocator<FragmentAllocator<FragmentAllocator.Default>>();
            _fragmentAllocator.Initialize(default(FragmentAllocator.Default));

            _fixedSizePoolAllocator = new FixedSizeAllocator<FixedSizePoolAllocator<FixedSizePoolAllocator.Default>>();
            _fixedSizePoolAllocator.Initialize(default(FixedSizePoolAllocator.Default));
        }

        [Benchmark(OperationsPerInvoke = OperationsPerRound)]
        public void Allocate_Native_SameSize()
        {
            for (int i = 0; i < OperationsPerRound; i++)
            {
                var ptr = _nativeAllocator.Allocate(1024);
                _nativeAllocator.Release(ref ptr);
            }
        }

        [Benchmark(OperationsPerInvoke = OperationsPerRound)]
        public void Allocate_Pool_SameSize()
        {
            for (int i = 0; i < OperationsPerRound; i++)
            {
                var ptr = _pooAllocator.Allocate(1024);
                _pooAllocator.Release(ref ptr);
            }
        }

        [Benchmark(OperationsPerInvoke = OperationsPerRound)]
        public void Allocate_Arena_SameSize()
        {
            for (int i = 0; i < OperationsPerRound; i++)
            {
                var ptr = _arenaAllocator.Allocate(1024);
                _arenaAllocator.Release(ref ptr);
            }
        }

        [Benchmark(OperationsPerInvoke = OperationsPerRound)]
        public void Allocate_Fragment_SameSize()
        {
            for (int i = 0; i < OperationsPerRound; i++)
            {
                var ptr = _fragmentAllocator.Allocate(1024);
                _fragmentAllocator.Release(ref ptr);
            }
        }

        [Benchmark(OperationsPerInvoke = OperationsPerRound)]
        public void Allocate_FixedSizePool_SameSize()
        {
            for (int i = 0; i < OperationsPerRound; i++)
            {
                var ptr = _fixedSizePoolAllocator.Allocate();
                _fixedSizePoolAllocator.Release(ref ptr);
            }
        }
    }
}

