using System;
using System.Linq;
using System.Runtime.CompilerServices;
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
    [DisassemblyDiagnoser(printSource:true, maxDepth:3, exportHtml:true)]
    [InliningDiagnoser(logFailuresOnly:true, filterByNamespace:true)]
    [Config(typeof(BatchedIntegerEncodingBench.Config))]
    public unsafe class BatchedIntegerEncodingBench
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
        private int[] _buffer = new int[1024];

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

        [Benchmark]
        public void ReadMany8()
        {
            ReadOnlySpan<byte> encodedSpan = _encodedNumbers;

            Span<int> local = _buffer;

            VariableSizeEncoding.ReadMany(encodedSpan, 8, local);
        }

        [Benchmark]
        public void ReadMany16()
        {
            ReadOnlySpan<byte> encodedSpan = _encodedNumbers;

            Span<int> local = _buffer;

            VariableSizeEncoding.ReadMany(encodedSpan, 16, local);
        }

        [Benchmark]
        public void ReadMany32()
        {
            ReadOnlySpan<byte> encodedSpan = _encodedNumbers;

            Span<int> local = _buffer;

            VariableSizeEncoding.ReadMany(encodedSpan, 32, local);
        }


        [Benchmark]
        public void ReadMany64()
        {
            ReadOnlySpan<byte> encodedSpan = _encodedNumbers;

            Span<int> local = _buffer;

            VariableSizeEncoding.ReadMany(encodedSpan, 64, local);
        }


        [Benchmark]
        public void ReadMany128()
        {
            ReadOnlySpan<byte> encodedSpan = _encodedNumbers;

            Span<int> local = _buffer;

            VariableSizeEncoding.ReadMany(encodedSpan, 128, local);
        }


        [Benchmark]
        public void ReadMany256()
        {
            ReadOnlySpan<byte> encodedSpan = _encodedNumbers;

            Span<int> local = _buffer;

            VariableSizeEncoding.ReadMany(encodedSpan, 256, local);
        }

        [Benchmark]
        public void ReadSequential4()
        {
            ReadOnlySpan<byte> encodedSpan = _encodedNumbers;

            int pos = 0;
            VariableSizeEncoding.Read<int>(encodedSpan, out int length, pos);
            pos += length;
            VariableSizeEncoding.Read<int>(encodedSpan, out length, pos);
            pos += length;
            VariableSizeEncoding.Read<int>(encodedSpan, out length, pos);
            pos += length;
            VariableSizeEncoding.Read<int>(encodedSpan, out length, pos);
        }

        [Benchmark]
        public void ReadSequential8()
        {
            ReadOnlySpan<byte> encodedSpan = _encodedNumbers;

            int pos = 0;
            VariableSizeEncoding.Read<int>(encodedSpan, out int length, pos);
            pos += length;
            VariableSizeEncoding.Read<int>(encodedSpan, out length, pos);
            pos += length;
            VariableSizeEncoding.Read<int>(encodedSpan, out length, pos);
            pos += length;
            VariableSizeEncoding.Read<int>(encodedSpan, out length, pos);
            pos += length;
            VariableSizeEncoding.Read<int>(encodedSpan, out length, pos);
            pos += length;
            VariableSizeEncoding.Read<int>(encodedSpan, out length, pos);
            pos += length;
            VariableSizeEncoding.Read<int>(encodedSpan, out length, pos);
            pos += length;
            VariableSizeEncoding.Read<int>(encodedSpan, out length, pos);
        }


    }
}
