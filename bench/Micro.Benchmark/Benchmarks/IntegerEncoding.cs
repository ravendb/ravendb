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
                        Runtime = CoreRuntime.Core50,
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
            var rnd = new Random(1337);
            for (int i = 0; i < Operations; i++)
                _numbersToEncode[i] = (uint)rnd.Next();

            Span<byte> encodedSpan = _encodedNumbers;
            for (int i = 0; i < Operations; i++)
            {
                var length = VariableSizeEncoding.Write(encodedSpan, _numbersToEncode[i]);
                encodedSpan.Slice(length);
            }
        }

        [Benchmark(OperationsPerInvoke = Operations)]
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
        public void CurrentEncoding()
        {
            Span<byte> dest = stackalloc byte[32];
            // assume that we don't use negative values very often
            foreach (uint value in _numbersToEncode)
            {
                VariableSizeEncoding.Write(dest, value);
            }
        }

        [Benchmark(OperationsPerInvoke = Operations)]
        public void CurrentDecoding()
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
        public void BasicDecoding()
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
    }
}
