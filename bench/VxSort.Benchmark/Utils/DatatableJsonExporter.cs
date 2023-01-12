using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Reports;
using Microsoft.CodeAnalysis.CSharp;
using JsonSerializer = Bench.Utils.SimpleJson;

namespace Bench.Utils
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Assembly, AllowMultiple = true)]
    public class DatatableJsonExporterAttribute : ExporterConfigBaseAttribute
    {
        private DatatableJsonExporterAttribute(IExporter exporter) : base(exporter) { }

        public DatatableJsonExporterAttribute(
            bool indentJson = false,
            bool excludeMeasurements = false
        ) : this(new DatatableJsonExporter(indentJson, excludeMeasurements)) { }
    }

    public class DatatableJsonExporter : ExporterBase
    {
        protected override string FileExtension => "datatable.json";

        bool IndentJson { get; } = true;
        private bool ExcludeMeasurements { get; }

        public DatatableJsonExporter(bool indentJson = false, bool excludeMeasurements = false)
        {
            IndentJson = indentJson;
            ExcludeMeasurements = excludeMeasurements;
        }

        public override void ExportToLog(Summary summary, ILogger logger)
        {
            var reportIndex = -1;
            var benchmarks = summary.Reports.Select(report =>
            {
                reportIndex++;

                var n = (int)report.BenchmarkCase.Parameters["N"];

                var data = new Dictionary<string, object>
                {
                    { "FullName", FullNameProvider.GetBenchmarkName(report.BenchmarkCase) }, // do NOT remove this property, it is used for xunit-performance migration
                    { "MethodName", FullNameProvider.GetMethodName(report.BenchmarkCase) },
                    { "Mean", report.ResultStatistics.Mean },
                    {
                        "MeanDataTable",
                        $"{report.ResultStatistics.Mean:0.0000} ({report.ResultStatistics.ConfidenceInterval.Lower:0.0000} - {report.ResultStatistics.ConfidenceInterval.Upper:0.0000})"
                    },
                    {
                        "TimePerNDataTable",
                        $"{report.ResultStatistics.Mean / n:0.0000} ({report.ResultStatistics.ConfidenceInterval.Lower / n:0.0000} - {report.ResultStatistics.ConfidenceInterval.Upper / n:0.0000})"
                    },
                    { "Median", report.ResultStatistics.Median },
                };

                var logicalGroups = summary.Table.FullContentStartOfLogicalGroup;

                foreach (var c in summary.Table.Columns)
                    data.Add(c.OriginalColumn.Id, summary.Table.FullContent[reportIndex][c.Index]);

                foreach (var param in report.BenchmarkCase.Parameters.Items)
                    data.Add(param.Name, param.Value);

                // We construct Measurements manually, so that we can have the IterationMode enum as text, rather than an integer

                var resultMeasurements = report.AllMeasurements
                    .Where(
                        m =>
                            m.IterationMode == IterationMode.Workload
                            && m.IterationStage == IterationStage.Result
                    )
                    .Select(m => m.Nanoseconds)
                    .ToArray();

                var min = resultMeasurements.Min();
                var max = resultMeasurements.Max();

                var measuremeantString = string.Join(
                    ",",
                    resultMeasurements.Select(
                        m =>
                            m switch
                            {
                                var x when x == min => $"{x};#00AA00",
                                var x when x == max => $"{x};#AA0000",
                                _ => m.ToString()
                            }
                    )
                );

                data.Add("Measurements", measuremeantString);

                if (report.Metrics.Any())
                {
                    data.Add("Metrics", report.Metrics.Values);
                }
                return data;
            });

            var flatData = benchmarks.ToArray();

            FixRatio(flatData);

            //JsonSerializer.CurrentJsonSerializerStrategy.Indent = IndentJson;
            JsonSerializer.CurrentJsonSerializerStrategy.Indent = true;
            logger.WriteLine(JsonSerializer.SerializeObject(flatData));
        }

        void FixRatio(Dictionary<string, object>[] flatData)
        {
            var maxRatio = flatData
                .Select(data => double.Parse((string)data["BaselineRatioColumn.Mean"]))
                .Max();

            foreach (var data in flatData)
            {
                var currentRatio = double.Parse((string)data["BaselineRatioColumn.Mean"]);

                var ratioColor =
                    currentRatio == 1
                        ? "#aaaaaa"
                        : currentRatio > 1
                            ? "#CC0000"
                            : "#00CC00";

                var ratio100 = (int)(currentRatio * 100);
                var ratio100Scaled = (int)((currentRatio / maxRatio) * 100);

                data.Add(
                    "RatioDataTable",
                    $"({ratio100}:{ratio100Scaled});{currentRatio * 100:N2}%;{ratioColor}"
                );
            }
        }
    }
}
