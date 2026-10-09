using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Mathematics;
using BenchmarkDotNet.Parameters;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

namespace Zstd.Benchmark.Infrastructure
{
    /// <summary>
    /// Implemented by benchmarks that know how many uncompressed bytes one operation processes.
    /// Evaluated in the host process on a fresh instance with the case's parameters applied (no setup is run).
    /// </summary>
    public interface IThroughputSource
    {
        double GetUncompressedBytesPerOperation(string method);
    }

    internal sealed class ThroughputColumn : IColumn
    {
        private readonly ConcurrentDictionary<BenchmarkCase, double?> _bytesPerOperation = new();

        public string Id => nameof(ThroughputColumn);
        public string ColumnName => "MB/s";
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 0;
        public bool IsNumeric => true;
        public UnitType UnitType => UnitType.Dimensionless;
        public string Legend => "Uncompressed MiB processed per second (mean)";

        public bool IsAvailable(Summary summary) => true;

        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase) => GetValue(summary, benchmarkCase, SummaryStyle.Default);

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style)
        {
            Statistics statistics = summary[benchmarkCase]?.ResultStatistics;
            if (statistics == null)
                return "NA";

            double? bytes = _bytesPerOperation.GetOrAdd(benchmarkCase, GetBytesPerOperation);
            if (bytes == null)
                return "-";

            double seconds = statistics.Mean / 1_000_000_000d;
            return (bytes.Value / seconds / (1024 * 1024)).ToString("N1", CultureInfo.InvariantCulture);
        }

        private static double? GetBytesPerOperation(BenchmarkCase benchmarkCase)
        {
            Type type = benchmarkCase.Descriptor.Type;
            if (typeof(IThroughputSource).IsAssignableFrom(type) == false)
                return null;

            object instance = Activator.CreateInstance(type);
            foreach (ParameterInstance parameter in benchmarkCase.Parameters.Items)
            {
                PropertyInfo property = type.GetProperty(parameter.Name);
                if (property != null)
                {
                    property.SetValue(instance, parameter.Value);
                    continue;
                }

                type.GetField(parameter.Name)?.SetValue(instance, parameter.Value);
            }

            return ((IThroughputSource)instance).GetUncompressedBytesPerOperation(benchmarkCase.Descriptor.WorkloadMethod.Name);
        }
    }
}
