using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow;
using Sparrow.Server.Tensors;

namespace Micro.Benchmark.Benchmarks.Tensors
{
    [DisassemblyDiagnoser(printSource: true, maxDepth: 3, exportHtml: true)]
    [Config(typeof(Config))]
    public class QuantizedCosineSimilarityBench
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
        //[Params(
        //    384  // Mini-LM
        //)]
        public int EmbeddingSize { get; set; }

        private static (sbyte[], sbyte[], short[], short[], int[], int[], float[], float[]) Generator(int size)
        {
            var gen = new Random(size);

            var resultSB1 = new sbyte[size];
            var resultSB2 = new sbyte[size];
            var resultS1 = new short[size];
            var resultS2 = new short[size];
            var resultI1 = new int[size];
            var resultI2 = new int[size];
            var resultF1 = new float[size];
            var resultF2 = new float[size];
            for (int i = 0; i < size; i++)
            {
                int value = gen.Next(4096);
                resultSB1[i] = (sbyte)(value % byte.MaxValue);
                resultS1[i] = (short)(value % ushort.MaxValue);
                resultI1[i] = value;
                resultF1[i] = value;

                value = gen.Next(4096);
                resultSB2[i] = (sbyte)(value % byte.MaxValue);
                resultS2[i] = (short)(value % ushort.MaxValue);
                resultI2[i] = value;
                resultF2[i] = value;
            }

            return (resultSB1, resultSB2, resultS1, resultS2, resultI1, resultI2, resultF1, resultF2);
        }

        private sbyte[] _sbyteValuesA;
        private sbyte[] _sbyteValuesB;
        private short[] _shortValuesA;
        private short[] _shortValuesB;
        private int[] _intValuesA;
        private int[] _intValuesB;
        private float[] _floatValuesA;
        private float[] _floatValuesB;
        private float _magnitudeA;
        private float _magnitudeB;

        [GlobalSetup]
        public void Setup()
        {
            (_sbyteValuesA, _sbyteValuesB, _shortValuesA, _shortValuesB, _intValuesA, _intValuesB, _floatValuesA, _floatValuesB) = Generator(EmbeddingSize);
            _magnitudeA = Random.Shared.NextSingle();
            _magnitudeB = Random.Shared.NextSingle();
        }

        [Benchmark(Baseline = true)]
        public float Unquantized_Float()
        {
            return Functions.CosineSimilarity<float, float>(_floatValuesA, _magnitudeA, _floatValuesB, _magnitudeB);
        }

        [Benchmark]
        public float Serial_SByte()
        {
            return Functions.Serial.CosineSimilarity<sbyte, float>(_sbyteValuesA, _magnitudeA, _sbyteValuesB, _magnitudeB);
        }

        [Benchmark]
        public float Avx2_SByteSerial()
        {
            return Functions.Vectorized256.CosineSimilarityIntegersAvx2(_sbyteValuesA, _magnitudeA, _sbyteValuesB, _magnitudeB);
        }

        [Benchmark]
        public float Avx512_SByteSerial()
        {
            return Functions.Vectorized512.CosineSimilarityIntegersAvx512(_sbyteValuesA, _magnitudeA, _sbyteValuesB, _magnitudeB);
        }
    }
}
