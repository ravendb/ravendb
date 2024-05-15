using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow;

namespace Micro.Benchmark.Benchmarks
{
    [Config(typeof(ThrowingAllocationConfig))]
    [InliningDiagnoser(true, true)]
    [MemoryDiagnoser]
    
    public class ThrowingAllocationBench
    {
        private class ThrowingAllocationConfig : ManualConfig
        {
            public ThrowingAllocationConfig()
            {
                AddJob(new Job
                {
                    Environment =
                    {
                        Runtime = CoreRuntime.Core80,
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

        private int location;
        
        [GlobalSetup]
        public void Setup()
        {
            var rnd = new Random();
            location = rnd.Next(1) + 998;
        }

        [Benchmark]
        public void Naked()
        {

            for (int i = 0; i < 1000; i++)
            {
                try
                {
                    if (location == i)
                        throw new InvalidOperationException($"We are constructing this based on the i value = {i}");
                }
                catch { }
            }
        }

        [DoesNotReturn]
        private void ThrowMethod(int i)
        {
            throw new InvalidOperationException($"We are constructing this based on the i value = {i}");
        }

        [Benchmark]
        public void NakedWithThrow()
        {

            for (int i = 0; i < 1000; i++)
            {
                try
                {
                    if (location == i)
                        ThrowMethod(i);
                }
                catch { }
            }
        }

        [Benchmark]
        public void PortableIfNoInterpolation()
        {

            for (int i = 0; i < 1000; i++)
            {
                try
                {
                    ThrowIf(location == i, $"We are constructing this based on the i value = i");
                }
                catch { }
            }
        }

        [Benchmark]
        public void PortableIfWithNoInlining()
        {

            for (int i = 0; i < 1000; i++)
            {
                try
                {
                    ThrowIf(location == i, $"We are constructing this based on the i value = {i}");
                }
                catch { }
            }
        }

        [Benchmark]
        public void PortableIfWithCondition()
        {

            for (int i = 0; i < 1000; i++)
            {
                try
                {
                    ThrowIfWithCondition(location == i, $"We are constructing this based on the i value = {i}");
                }
                catch { }
            }
        }

        [Benchmark]
        public void PortableIfDelegate()
        {

            for (int i = 0; i < 1000; i++)
            {
                try
                {
                    ThrowIf(location == i, () => $"We are constructing this based on the i value = {i}");
                }
                catch { }
            }
        }

        [Benchmark]
        public void OutOfRange()
        {
            for (int i = 0; i < 1000; i++)
            {
                try
                {
                    ArgumentOutOfRangeException.ThrowIfEqual(i, location);
                }
                catch { }
            }
        }


        public delegate string ThrowMessage();
        
        public static void ThrowIf(
            bool condition,
            ThrowMessage message)
        {
            if (condition)
            {
                throw new InvalidOperationException(message());
            }
        }

        public static void ThrowIfWithCondition(
            [DoesNotReturnIf(true)] bool condition,
            string message)
        {
            if (condition)
            {
                throw new InvalidOperationException(message);
            }
        }

        public static void ThrowIf(
            bool condition,
            string message,
            [CallerArgumentExpression(nameof(condition))]
            string paramName = null)
        {
            if (condition)
            {
                throw new InvalidOperationException(message);
            }
        }
        
        

        [InterpolatedStringHandler]
        public readonly ref struct ThrowInterpolatedStringHandler
        {
            // Storage for the built-up string
            private readonly List<object> _strings;

            public ThrowInterpolatedStringHandler(int literalLength, int formattedCount)
            {
                _strings = new List<object>(literalLength);
            }

            public void AppendLiteral(string s)
            {
                _strings.Add(s);
            }

            public void AppendFormatted<T>(T t)
            {
                _strings.Add(t);
            }

            internal string GetFormattedText()
            {
                var builder = new StringBuilder();
                foreach (var obj in _strings)
                    builder.Append(obj);
                
                return builder.ToString();
            }
        }

        public static void ThrowIfInterpolated(
            bool condition,
            ThrowInterpolatedStringHandler message,
            [CallerArgumentExpression(nameof(condition))]
            string paramName = null)
        {
            if (condition)
            {
                throw new InvalidOperationException(message.GetFormattedText());
            }
        }

        [Benchmark]
        public void PortableIfInterpolated()
        {

            for (int i = 0; i < 1000; i++)
            {
                try
                {
                    ThrowIfInterpolated(location == i, $"We are constructing this based on the i value = {i}");
                }
                catch { }
            }
        }


        public static void ThrowIfInterpolatedEfficient(
            bool condition,
            [InterpolatedStringHandlerArgument(nameof(condition))]
            ThrowInterpolatedStringEfficientHandler message,
            [CallerArgumentExpression(nameof(condition))]
            string paramName = null)
        {
            if (condition)
            {
                throw new InvalidOperationException(message.GetFormattedText());
            }
        }
        
        [InterpolatedStringHandler]
        public readonly ref struct ThrowInterpolatedStringEfficientHandler
        {
            private readonly StringBuilder? _builder;

            public ThrowInterpolatedStringEfficientHandler(int literalLength, int formattedCount, bool condition, out bool isEnabled)
            {
                isEnabled = condition;
                _builder = condition ? new StringBuilder(literalLength + formattedCount * 2 + 1) : null;
            }

            public void AppendLiteral(string s)
            {
                _builder!.Append(s);
            }

            public void AppendFormatted<T>(T t)
            {
                _builder!.Append(t);
            }

            internal string? GetFormattedText()
            {
                return _builder!.ToString();
            }
        }

        [Benchmark]
        public void PortableIfInterpolatedEfficient()
        {

            for (int i = 0; i < 1000; i++)
            {
                try
                {
                    ThrowIfInterpolatedEfficient(location == i, $"We are constructing this based on the i value = {i}");
                }
                catch { }
            }
        }

    }
}
