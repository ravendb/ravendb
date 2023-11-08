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
    [Config(typeof(XXHash64Bench.Config))]
    public unsafe class XXHash64Bench
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
            return Hashing.XXHash64.CalculateInline(_bufferPtr.Ptr, (ulong)_bufferPtr.Length, 1337);
        }

        [Benchmark]
        public ulong XXHash64_FixedPointer()
        {
            int len = _buffer.Length;

            fixed (byte* buffer = _buffer)
            {
                return Hashing.XXHash64.CalculateInline(buffer, (ulong)len, 1337);
            }
        }

        [Benchmark]
        public ulong XXHash64_FixedSpan()
        {
            int len = _buffer.Length;

            fixed (byte* buffer = _buffer)
            {
                return Hashing.XXHash64.CalculateInline(new ReadOnlySpan<byte>(buffer, len), 1337);
            }
        }

        [Benchmark]
        public ulong XXHash64_Span()
        {
            return Hashing.XXHash64.CalculateInline(_buffer.AsSpan(), 1337);
        }

        [Benchmark]
        public ulong XXHash64_SpanFromPointer()
        {
            return Hashing.XXHash64.CalculateInline(new ReadOnlySpan<byte>(_bufferPtr.Ptr, _bufferPtr.Length), 1337);
        }

        [Benchmark]
        public ulong XXHash64_StreamedWhole()
        {
            var processor = new Hashing.Streamed.XXHash64Processor(1337);
            processor.Process(new ReadOnlySpan<byte>(_bufferPtr.Ptr, _bufferPtr.Length));
            return processor.End();
        }

        [Benchmark]
        public ulong XXHash64_StreamedMultiple()
        {
            var processor = new Hashing.Streamed.XXHash64Processor(1337);
            processor.Process(new ReadOnlySpan<byte>(_bufferPtr.Ptr, _bufferPtr.Length - 32));
            processor.Process(new ReadOnlySpan<byte>(_bufferPtr.Ptr + _bufferPtr.Length - 32,32));
            return processor.End();
        }

        [Benchmark(Baseline = true)]
        public ulong XXHash64_Reference()
        {
            return XXHash64Reference(_bufferPtr.Ptr, (ulong)_bufferPtr.Length, 1337);
        }

        private static ulong XXHash64Reference(byte* buffer, ulong len, ulong seed = 0)
        {
            ulong h64;

            byte* bEnd = buffer + len;

            if (len >= 32)
            {
                byte* limit = bEnd - 32;

                ulong v1 = seed + Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_2;
                ulong v2 = seed + Hashing.XXHash64Constants.PRIME64_2;
                ulong v3 = seed + 0;
                ulong v4 = seed - Hashing.XXHash64Constants.PRIME64_1;

                do
                {
                    v1 += ((ulong*)buffer)[0] * Hashing.XXHash64Constants.PRIME64_2;
                    v2 += ((ulong*)buffer)[1] * Hashing.XXHash64Constants.PRIME64_2;
                    v3 += ((ulong*)buffer)[2] * Hashing.XXHash64Constants.PRIME64_2;
                    v4 += ((ulong*)buffer)[3] * Hashing.XXHash64Constants.PRIME64_2;

                    buffer += 4 * sizeof(ulong);

                    v1 = Bits.RotateLeft64(v1, 31);
                    v2 = Bits.RotateLeft64(v2, 31);
                    v3 = Bits.RotateLeft64(v3, 31);
                    v4 = Bits.RotateLeft64(v4, 31);

                    v1 *= Hashing.XXHash64Constants.PRIME64_1;
                    v2 *= Hashing.XXHash64Constants.PRIME64_1;
                    v3 *= Hashing.XXHash64Constants.PRIME64_1;
                    v4 *= Hashing.XXHash64Constants.PRIME64_1;
                }
                while (buffer <= limit);

                h64 = Bits.RotateLeft64(v1, 1) + Bits.RotateLeft64(v2, 7) + Bits.RotateLeft64(v3, 12) + Bits.RotateLeft64(v4, 18);

                v1 *= Hashing.XXHash64Constants.PRIME64_2;
                v2 *= Hashing.XXHash64Constants.PRIME64_2;
                v3 *= Hashing.XXHash64Constants.PRIME64_2;
                v4 *= Hashing.XXHash64Constants.PRIME64_2;

                v1 = Bits.RotateLeft64(v1, 31);
                v2 = Bits.RotateLeft64(v2, 31);
                v3 = Bits.RotateLeft64(v3, 31);
                v4 = Bits.RotateLeft64(v4, 31);

                v1 *= Hashing.XXHash64Constants.PRIME64_1;
                v2 *= Hashing.XXHash64Constants.PRIME64_1;
                v3 *= Hashing.XXHash64Constants.PRIME64_1;
                v4 *= Hashing.XXHash64Constants.PRIME64_1;

                h64 ^= v1;
                h64 = h64 * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;

                h64 ^= v2;
                h64 = h64 * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;

                h64 ^= v3;
                h64 = h64 * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;

                h64 ^= v4;
                h64 = h64 * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;
            }
            else
            {
                h64 = seed + Hashing.XXHash64Constants.PRIME64_5;
            }

            h64 += (ulong)len;


            while (buffer + 8 <= bEnd)
            {
                ulong k1 = *((ulong*)buffer);
                k1 *= Hashing.XXHash64Constants.PRIME64_2;
                k1 = Bits.RotateLeft64(k1, 31);
                k1 *= Hashing.XXHash64Constants.PRIME64_1;
                h64 ^= k1;
                h64 = Bits.RotateLeft64(h64, 27) * Hashing.XXHash64Constants.PRIME64_1 + Hashing.XXHash64Constants.PRIME64_4;
                buffer += 8;
            }

            if (buffer + 4 <= bEnd)
            {
                h64 ^= *(uint*)buffer * Hashing.XXHash64Constants.PRIME64_1;
                h64 = Bits.RotateLeft64(h64, 23) * Hashing.XXHash64Constants.PRIME64_2 + Hashing.XXHash64Constants.PRIME64_3;
                buffer += 4;
            }

            while (buffer < bEnd)
            {
                h64 ^= ((ulong)*buffer) * Hashing.XXHash64Constants.PRIME64_5;
                h64 = Bits.RotateLeft64(h64, 11) * Hashing.XXHash64Constants.PRIME64_1;
                buffer++;
            }

            h64 ^= h64 >> 33;
            h64 *= Hashing.XXHash64Constants.PRIME64_2;
            h64 ^= h64 >> 29;
            h64 *= Hashing.XXHash64Constants.PRIME64_3;
            h64 ^= h64 >> 32;

            return h64;
        }
    }
}
