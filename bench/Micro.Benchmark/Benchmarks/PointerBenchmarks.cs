using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow.Binary;
using Sparrow.Compression;

namespace Micro.Benchmark.Benchmarks
{
    [Config(typeof(PointerBenchmarks.Config))]
    public unsafe class PointerBenchmarks
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
                        Jit = Jit.Default,
                    },
                    Run =
                    {
                        // TODO: Next line is just for testing. Fine tune parameters.
                        //RunStrategy = RunStrategy.Monitoring,
                    }
                });

                // Exporters for data
                AddExporter(GetExporters().ToArray());

                AddValidator(BaselineValidator.FailOnError);
                AddValidator(JitOptimizationsValidator.FailOnError);

                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        private const int Operations = 10000;
        private int[] _bitsToFlip = new int[Operations];
        private byte[] _backingStorage = new byte[4096];
        
        private GCHandle _backingStorageHandle;
        private IntPtr _backingStoragePinned;

        [GlobalSetup]
        public void Setup()
        {
            // We will try to follow the usual distribution we have during encoding. 
            var rnd = new Random(1337);
            for (int i = 0; i < Operations; i++)
            {
                _bitsToFlip[i] = rnd.Next(_backingStorage.Length * Bits.InByte);
            }

            _backingStorageHandle = GCHandle.Alloc(_backingStorage, GCHandleType.Pinned);

            // retrieve a raw pointer to pass to the native code:
            _backingStoragePinned = _backingStorageHandle.AddrOfPinnedObject();
        }

        [GlobalCleanup]
        public void OnFinalize()
        {
            _backingStorageHandle.Free();
        }

        [Benchmark(Baseline = true)]
        public void Current()
        {
            BitVector vector = new(_backingStorage.AsSpan());
            foreach (var bit in _bitsToFlip)
            {
                bool value = vector.Get(bit);
                vector.Set(bit, !value);
            }
        }

        [Benchmark]
        public void CurrentPtr()
        {
            PtrBitVector vector = new(_backingStoragePinned.ToPointer(), 4096 * Bits.InByte);
            foreach (var bit in _bitsToFlip)
            {
                bool value = vector.Get(bit);
                vector.Set(bit, !value);
            }
        }
    }
}
