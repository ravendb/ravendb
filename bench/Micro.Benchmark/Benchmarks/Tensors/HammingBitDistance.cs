using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics.Tensors;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow.Server.Tensors;



namespace Micro.Benchmark.Benchmarks.Tensors
{
    [DisassemblyDiagnoser(printSource: true, maxDepth: 3, exportHtml: true)]
    [Config(typeof(Config))]
    public class HammingBitDistanceBench
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job { Environment = { Runtime = CoreRuntime.Core80, Platform = Platform.X64, Jit = Jit.RyuJit, }, });

                // Exporters for data
                AddExporter(GetExporters().ToArray());

                AddValidator(BaselineValidator.FailOnError);
                AddValidator(JitOptimizationsValidator.FailOnError);

                AddAnalyser(EnvironmentAnalyser.Default);
            }
        }

        [Params(
            384,  // Mini-LM,
            1536, // OpenAI
            3072, // text-embedding-3-large, Gemini
            4096, // Llama 3 
            8192 // Llama 2 70B
        )]
        public int EmbeddingSize { get; set; }

        private static (byte[], byte[], long[], long[]) Generator(int size)
        {
            var gen = new Random(size);

            var resultF1 = new byte[size * sizeof(long)];
            var resultF2 = new byte[size * sizeof(long)];
            var resultD1 = new long[size];
            var resultD2 = new long[size];
            for (int i = 0; i < size; i++)
            {
                byte value = (byte)gen.Next(byte.MaxValue);
                resultF1[i * sizeof(long)] = value;
                resultD1[i] = value;

                value = (byte)gen.Next(byte.MaxValue);
                resultF2[i * sizeof(long)] = value;
                resultD2[i] = value;
            }

            return (resultF1, resultF2, resultD1, resultD2);
        }

        private byte[] _byteValuesA;
        private byte[] _byteValuesB;
        private long[] _longValuesA;
        private long[] _longValuesB;


        [GlobalSetup]
        public void Setup()
        {
            (_byteValuesA, _byteValuesB, _longValuesA, _longValuesB) = Generator(EmbeddingSize);
        }

        [Benchmark(Baseline = true)]
        public long Microsoft_TensorPrimitives_Byte()
        {
            return TensorPrimitives.HammingBitDistance<byte>(_byteValuesA, _byteValuesB);
        }

        [Benchmark]
        public long Microsoft_TensorPrimitives_Long()
        {
            return TensorPrimitives.HammingBitDistance<long>(_longValuesA, _longValuesB);
        }

        [Benchmark]
        public long Ours_Byte()
        {
            return Functions.HammingBitDistance<byte>(_byteValuesA, _byteValuesB);
        }

        [Benchmark]
        public double Ours_Long()
        {
            return Functions.HammingBitDistance<long>(_longValuesA, _longValuesB);
        }
    }
}
