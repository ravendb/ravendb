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
    public class CosineSimilarityBench
    {
        private class Config : ManualConfig
        {
            public Config()
            {
                AddJob(new Job { Environment = { Runtime = CoreRuntime.Core10_0, Platform = Platform.X64, Jit = Jit.RyuJit, }, });

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

        private static (float[], float[], double[], double[]) Generator(int size)
        {
            var gen = new Random(size);

            var resultF1 = new float[size];
            var resultF2 = new float[size];
            var resultD1 = new double[size];
            var resultD2 = new double[size];
            for (int i = 0; i < size; i++)
            {
                double value = gen.NextDouble();
                resultF1[i] = (float)value;
                resultD1[i] = value;
                value = gen.NextDouble();
                resultF2[i] = (float)value;
                resultD2[i] = value;
            }

            return (resultF1, resultF2, resultD1, resultD2);
        }

        private float[] _floatValuesA;
        private float[] _floatValuesB;
        private double[] _doubleValuesA;
        private double[] _doubleValuesB;


        [GlobalSetup]
        public void Setup()
        {
            (_floatValuesA, _floatValuesB, _doubleValuesA, _doubleValuesB) = Generator(EmbeddingSize);
        }

        [Benchmark(Baseline = true)]
        public float Microsoft_TensorPrimitives_Float()
        {
            return TensorPrimitives.CosineSimilarity(_floatValuesA, _floatValuesB);
        }

        [Benchmark]
        public float Serial_Float()
        {
            return Functions.Serial.CosineSimilarity<float, float>(_floatValuesA, _floatValuesB);
        }

        [Benchmark]
        public double Serial_Double()
        {
            return Functions.Serial.CosineSimilarity<double, double>(_doubleValuesA, _doubleValuesB);
        }

        [Benchmark]
        public float Fma_SingleLane_Float()
        {
            return Functions.Vectorized256.CosineSimilarity<float, float>(_floatValuesA, _floatValuesB);
        }

        [Benchmark]
        public float Fma_DoubleLane_Float()
        {
            return Functions.Vectorized512.CosineSimilarity<float, float>(_floatValuesA, _floatValuesB);
        }

        [Benchmark]
        public double Fma_DoubleLane_Double()
        {
            return Functions.Vectorized512.CosineSimilarity<double, double>(_doubleValuesA, _doubleValuesB);
        }
    }
}
