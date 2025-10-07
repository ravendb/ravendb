using System.Threading.Tasks;
using BenchmarkDotNet.Running;

namespace Requests.Benchmark;

class Program
{
    static
#if DEBUG
        async Task
#else
        void
#endif
        Main(string[] args)
    {
#if DEBUG
        var benchmark = new QueriesPostHandlerBenchmark();
        benchmark.Setup();
        await benchmark.Query();
        benchmark.Cleanup();
#else
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, new Config());
#endif
    }
}
