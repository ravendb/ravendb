using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Loggers;

namespace Requests.Benchmark;

public class Config : ManualConfig
{
    public Config()
    {
        AddLogger(ConsoleLogger.Default);
        WithOptions(ConfigOptions.DisableOptimizationsValidator);
        AddExporter(BenchmarkDotNet.Exporters.DefaultExporters.Markdown);
        AddColumnProvider(BenchmarkDotNet.Columns.DefaultColumnProviders.Instance);
    }
}
