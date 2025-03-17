using System.Linq;
using System.Text;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Validators;
using Sparrow.Json;

namespace Micro.Benchmark.Benchmarks.Parsing;

[Config(typeof(Config))]
public class DateTimeParsing
{
    private class Config : ManualConfig
    {
        public Config()
        {
            AddJob(new Job { Environment = { Runtime = CoreRuntime.Core80, Platform = Platform.X64, Jit = Jit.RyuJit, } });

            // Exporters for data
            AddExporter(GetExporters().ToArray());

            AddColumn(StatisticColumn.AllStatistics);

            AddValidator(BaselineValidator.FailOnError);
            AddValidator(JitOptimizationsValidator.FailOnError);

            AddAnalyser(EnvironmentAnalyser.Default);
        }
    }

    private const int NumberOfOperations = 10000;
    private byte[][] _dates;

    [GlobalSetup]
    public void Setup()
    {
        _dates =
        [
            "2016-10-05T21:07:32.2082285Z"u8.ToArray(),
            "2016-10-05T21:07:32.2082285+03:00"u8.ToArray(),
            "2024-12-13T02:38:42.786481Z"u8.ToArray(),
            "2016-10-05T21:07:32"u8.ToArray(),
            "2021-12-12T10:34:23.838"u8.ToArray(),
            "2021-12-12T10:34:23.838Z"u8.ToArray(),
            "2015-10-17T13:28:17-05:00"u8.ToArray()
        ];
    }

    [Benchmark(OperationsPerInvoke = NumberOfOperations)]
    public unsafe void TryParseDateTimeBenchmark()
    {
        fixed (byte* buffer1 = _dates[0])
        fixed (byte* buffer2 = _dates[1])
        fixed (byte* buffer3 = _dates[2])
        fixed (byte* buffer4 = _dates[3])
        fixed (byte* buffer5 = _dates[4])
        fixed (byte* buffer6 = _dates[5])
        fixed (byte* buffer7 = _dates[6])
        {
            var buffers = new[] { buffer1, buffer2, buffer3, buffer4, buffer5, buffer6, buffer7 };

            for (int i = 0; i < NumberOfOperations; i++)
            {
                int index = i % buffers.Length;

                LazyStringParser.TryParseDateTime(buffers[index], _dates[index].Length, out var time, out var dto, properlyParseThreeDigitsMilliseconds: true);
            }
        }
    }
}
