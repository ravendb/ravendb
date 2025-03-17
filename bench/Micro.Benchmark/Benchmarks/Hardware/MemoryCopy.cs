using System;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Server.Platform;
using Sparrow.Threading;
using RunMode = BenchmarkDotNet.Jobs.RunMode;

namespace Micro.Benchmark.Benchmarks.Hardware
{
    //[HardwareCounters(HardwareCounter.CacheMisses, HardwareCounter.TotalIssues, HardwareCounter.BranchMispredictions, HardwareCounter.InstructionRetired )]
    [DisassemblyDiagnoser]
    [Config(typeof(MemoryCopy.Config))]
    public unsafe class MemoryCopy
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job(RunMode.Default)
                {
                    Environment =
                    {
                        Runtime = CoreRuntime.Core80,
                        Platform = Platform.X64,
                        Jit = Jit.RyuJit
                    }
                });

                AddExporter(GetExporters().ToArray());

                AddValidator(BaselineValidator.FailOnError);
                AddValidator(JitOptimizationsValidator.FailOnError);

                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        [Params(7, 8, 16, 127, 128, 4096, 4096 * 64, 4096 * 256, 4096 * 1024)]
        public int Length { get; set; }

        private ByteStringContext _context;
        private readonly int _size = 4096 * 1024;
        private ByteString _source;
        private ByteString _destination;

        public const int VectorBytes = 32;

        [GlobalSetup]
        public void Setup()
        {
            if (Vector<byte>.Count != VectorBytes)
                throw new NotSupportedException("");

            _context = new ByteStringContext(SharedMultipleUseFlag.None);

            _context.Allocate(_size, out _source);
            _context.Allocate(_size, out _destination);

            var r = new Random();
            for (int i = 0; i < _size; i++)
            {
                int b = r.Next();
                _source.Ptr[i] = (byte)b;
            }
        }

        [Benchmark]
        public void SpanHelpers()
        {
            int length = Length;
            new Span<byte>(_source.Ptr, length).CopyTo(new Span<byte>(_destination.Ptr, length));
        }

        [Benchmark]
        public void CopyBlock()
        {
            Unsafe.CopyBlock(_destination.Ptr, _source.Ptr, (uint)Length);
        }

        [Benchmark]
        public void CopyBlockUnaligned()
        {
            Unsafe.CopyBlockUnaligned(_destination.Ptr, _source.Ptr, (uint)Length);
        }

        [Benchmark]
        public void BufferCopy()
        {
            int length = Length;
            Buffer.MemoryCopy(_source.Ptr, _destination.Ptr, length, length);
        }

    }
}
