using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow.Compression;
using Sparrow.Json;
using Sparrow.Server.Compression;

namespace Micro.Benchmark.Benchmarks
{

    [Config(typeof(IntegerEncodingBench.Config))]
    public unsafe class IntegerEncodingBench
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job
                {
                    Environment =
                    {
                        Runtime = CoreRuntime.Core60,
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

        private const int Operations = 10000;
        private uint[] _numbersToEncode = new uint[Operations];
        private byte[] _encodedNumbers = new byte[Operations * 5];

        [GlobalSetup]
        public void Setup()
        {
            // We will try to follow the usual distribution we have during encoding. 
            var rnd = new Random(1337);
            for (int i = 0; i < Operations; i++)
            {
                int distribution = rnd.Next(100);
                if (distribution < 50)
                    _numbersToEncode[i] = (uint)rnd.Next(128);
                else if (distribution < 75)
                    _numbersToEncode[i] = (uint)rnd.Next(256);
                else if (distribution < 90)
                    _numbersToEncode[i] = (uint)rnd.Next((int)short.MaxValue);
                else
                    _numbersToEncode[i] = (uint)rnd.Next((int)int.MaxValue);
            }

            Span<byte> encodedSpan = _encodedNumbers;
            for (int i = 0; i < Operations; i++)
            {
                var length = VariableSizeEncoding.Write(encodedSpan, _numbersToEncode[i]);
                encodedSpan.Slice(length);
            }
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        [SkipLocalsInit]
        public void BasicEncoding()
        {
            byte* src = stackalloc byte[32];

            // assume that we don't use negative values very often
            foreach (uint value in _numbersToEncode)
            {
                byte* dest = src;
                uint v = value;
                while (v >= 0x80)
                {
                    *dest++ = (byte)(v | 0x80);
                    v >>= 7;
                }
                *dest++ = (byte)(v);
            }
        }

        private const ulong ContinueMask = 0xFFFF_FFFF_FFFF_FF80;

        [Benchmark(OperationsPerInvoke = Operations)]
        [SkipLocalsInit]
        public void UnrollEncoding()
        {
            Span<byte> dest = stackalloc byte[32];

            // assume that we don't use negative values very often
            foreach (uint value in _numbersToEncode)
            {
                int i = 0;
                var auxDest = dest;
                ulong ul = value;

                // We do loop unrolling manually.

                if ((ul & ContinueMask) == 0)
                    goto End;
                auxDest[0] = (byte)(ul | 0x80);
                ul >>= 7;
                i++;

                if ((ul & ContinueMask) == 0)
                    goto End;
                auxDest[1] = (byte)(ul | 0x80);
                ul >>= 7;
                i++;

                if ((ul & ContinueMask) == 0)
                    goto End;
                auxDest[2] = (byte)(ul | 0x80);
                ul >>= 7;
                i++;

                if ((ul & ContinueMask) == 0)
                    goto End;
                auxDest[3] = (byte)(ul | 0x80);
                ul >>= 7;
                i++;

            End:
                auxDest[i] = (byte)ul;
            }
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        [SkipLocalsInit]
        public void CurrentEncodingPointer()
        {
            byte* dest = stackalloc byte[32];
            // assume that we don't use negative values very often
            foreach (uint value in _numbersToEncode)
            {
                VariableSizeEncoding.Write(dest, value);
            }
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        [SkipLocalsInit]
        public void CurrentEncodingSpan()
        {
            Span<byte> dest = stackalloc byte[32];
            // assume that we don't use negative values very often
            foreach (uint value in _numbersToEncode)
            {
                VariableSizeEncoding.Write(dest, value);
            }
        }


        [Benchmark(OperationsPerInvoke = Operations)]
        public void CurrentDecodingSpan()
        {
            ReadOnlySpan<byte> encodedSpan = _encodedNumbers;
            int pos = 0;
            for (int i = 0; i < Operations; i++)
            {
                VariableSizeEncoding.Read<int>(encodedSpan, out int length, pos);
                pos += length;
            }
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public void CurrentDecodingPointer()
        {
            fixed (byte* buffer = _encodedNumbers)
            {
                int pos = 0;
                for (int i = 0; i < Operations; i++)
                {
                    VariableSizeEncoding.Read<int>(buffer + pos, out int length);
                    pos += length;
                }
            }
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public void CurrentDecodingCompactFixedEveryCall()
        {
            ReadOnlySpan<byte> encodedSpan = _encodedNumbers;
            int pos = 0;
            for (int i = 0; i < Operations; i++)
            {
                fixed (byte* buffer = encodedSpan)
                {
                    VariableSizeEncoding.ReadCompact<int>(buffer + pos, out int length, out var success);
                    pos += length;
                }
            }
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public void CurrentDecodingCompactPointer()
        {
            fixed (byte* buffer = _encodedNumbers)
            {
                int pos = 0;
                for (int i = 0; i < Operations; i++)
                {
                    VariableSizeEncoding.ReadCompact<int>(buffer + pos, out int length, out var success);
                    pos += length;
                }
            }
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public void BlittableJsonDecoding()
        {
            fixed (byte* encodedPtr = _encodedNumbers)
            {
                int pos = 0;
                for (int i = 0; i < Operations; i++)
                {
                    BlittableJsonReaderBase.ReadVariableSizeInt(encodedPtr, pos, out var offset);
                    pos += offset;
                }
            }
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public void BlittableJsonDecodingAlt()
        {
            fixed (byte* encodedPtr = _encodedNumbers)
            {
                int pos = 0;
                for (int i = 0; i < Operations; i++)
                {
                    ReadVariableSizeAlt(encodedPtr, pos, out var offset, out _);
                    pos += offset;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadVariableSizeAlt(byte* buffer, int pos, out byte offset, out bool success)
        {
            offset = 0;

            if (pos < 0)
                goto Error;

            // Read out an Int32 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.
            // we assume that the value shouldn't be zero very often
            // because then we'll always take 5 bytes to store it

            ulong count = 0;
            byte shift = 0;
            byte b;
            do
            {
                if (shift == 35)
                    goto Error; // PERF: Using goto to diminish the size of the loop.

                b = buffer[pos];
                pos++;
                offset++;

                count |= (b & 0x7Ful) << shift;
                shift += 7;
            }
            while (b >= 0x80);

            success = true;
            return (int)count;

            Error:
            success = false;
            return -1;
        }


    }
}
