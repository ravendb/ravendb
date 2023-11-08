using System;
using System.Linq;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Threading;

namespace Micro.Benchmark.Benchmarks
{

    [DisassemblyDiagnoser(printSource: true, maxDepth: 3, exportHtml: true)]
    [InliningDiagnoser(logFailuresOnly: true, filterByNamespace: true)]
    [Config(typeof(XXHash32Bench.Config))]
    public unsafe class XXHash32Bench
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job
                {
                    Environment = { Runtime = CoreRuntime.Core70, Platform = Platform.X64, Jit = Jit.RyuJit, },
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

        private ByteStringContext _context;
        private byte[] _buffer = new byte[1024 * 8];
        private ByteString _bufferPtr;

        [GlobalSetup]
        public void Setup()
        {
            var rnd = new Random(1337);
            rnd.NextBytes(_buffer);

            _context = new ByteStringContext(SharedMultipleUseFlag.None);
            _context.Allocate(1024 * 8, out _bufferPtr);
        }

        [Benchmark]
        public ulong XXHash64_RawPointer()
        {
            return Hashing.XXHash32.CalculateInline(_bufferPtr.Ptr, _bufferPtr.Length, 1337);
        }

        [Benchmark]
        public ulong XXHash64_FixedPointer()
        {
            int len = _buffer.Length;

            fixed (byte* buffer = _buffer)
            {
                return Hashing.XXHash32.CalculateInline(buffer, len, 1337);
            }
        }

        [Benchmark]
        public ulong XXHash32_FixedSpan()
        {
            int len = _buffer.Length;

            fixed (byte* buffer = _buffer)
            {
                return Hashing.XXHash32.CalculateInline(new ReadOnlySpan<byte>(buffer, len), 1337);
            }
        }

        [Benchmark]
        public ulong XXHash32_Span()
        {
            return Hashing.XXHash32.CalculateInline(_buffer.AsSpan(), 1337);
        }

        [Benchmark]
        public ulong XXHash32_SpanFromPointer()
        {
            return Hashing.XXHash32.CalculateInline(new ReadOnlySpan<byte>(_bufferPtr.Ptr, _bufferPtr.Length), 1337);
        }

        [Benchmark(Baseline = true)]
        public ulong XXHash32_Reference()
        {
            return XXHash32Reference(_bufferPtr.Ptr, _bufferPtr.Length, 1337);
        }

        private static uint XXHash32Reference(byte* buffer, int len, uint seed = 0)
        {
            unchecked
            {
                uint h32;

                byte* bEnd = buffer + len;

                if (len >= 16)
                {
                    byte* limit = bEnd - 16;

                    uint v1 = seed + Hashing.XXHash32Constants.PRIME32_1 + Hashing.XXHash32Constants.PRIME32_2;
                    uint v2 = seed + Hashing.XXHash32Constants.PRIME32_2;
                    uint v3 = seed + 0;
                    uint v4 = seed - Hashing.XXHash32Constants.PRIME32_1;

                    do
                    {
                        v1 += ((uint*)buffer)[0] * Hashing.XXHash32Constants.PRIME32_2;
                        v2 += ((uint*)buffer)[1] * Hashing.XXHash32Constants.PRIME32_2;
                        v3 += ((uint*)buffer)[2] * Hashing.XXHash32Constants.PRIME32_2;
                        v4 += ((uint*)buffer)[3] * Hashing.XXHash32Constants.PRIME32_2;

                        buffer += 4 * sizeof(uint);

                        v1 = Bits.RotateLeft32(v1, 13);
                        v2 = Bits.RotateLeft32(v2, 13);
                        v3 = Bits.RotateLeft32(v3, 13);
                        v4 = Bits.RotateLeft32(v4, 13);

                        v1 *= Hashing.XXHash32Constants.PRIME32_1;
                        v2 *= Hashing.XXHash32Constants.PRIME32_1;
                        v3 *= Hashing.XXHash32Constants.PRIME32_1;
                        v4 *= Hashing.XXHash32Constants.PRIME32_1;
                    }
                    while (buffer <= limit);

                    h32 = Bits.RotateLeft32(v1, 1) + Bits.RotateLeft32(v2, 7) + Bits.RotateLeft32(v3, 12) + Bits.RotateLeft32(v4, 18);
                }
                else
                {
                    h32 = seed + Hashing.XXHash32Constants.PRIME32_5;
                }

                h32 += (uint)len;

                while (buffer + 4 <= bEnd)
                {
                    h32 += *((uint*)buffer) * Hashing.XXHash32Constants.PRIME32_3;
                    h32 = Bits.RotateLeft32(h32, 17) * Hashing.XXHash32Constants.PRIME32_4;
                    buffer += 4;
                }

                while (buffer < bEnd)
                {
                    h32 += (uint)(*buffer) * Hashing.XXHash32Constants.PRIME32_5;
                    h32 = Bits.RotateLeft32(h32, 11) * Hashing.XXHash32Constants.PRIME32_1;
                    buffer++;
                }

                h32 ^= h32 >> 15;
                h32 *= Hashing.XXHash32Constants.PRIME32_2;
                h32 ^= h32 >> 13;
                h32 *= Hashing.XXHash32Constants.PRIME32_3;
                h32 ^= h32 >> 16;

                return h32;
            }
        }
    }
}
