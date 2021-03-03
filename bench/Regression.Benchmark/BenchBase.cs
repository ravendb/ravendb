using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Regression.Benchmark
{
    public class BenchBase
    {
        protected void ExecuteBenchmark( Action action )
        {
            if ( Debugger.IsAttached )
            {
                action();
            }
            else
            {
                foreach (var iteration in Microsoft.Xunit.Performance.Benchmark.Iterations)
                {
                    using (iteration.StartMeasurement())
                    {
                        action();
                    }
                }
            }
        }

        protected async Task ExecuteBenchmarkAsync(Func<Task> action)
        {
            if (Debugger.IsAttached)
            {
                await action();
            }
            else
            {
                foreach (var iteration in Microsoft.Xunit.Performance.Benchmark.Iterations)
                {
                    using (iteration.StartMeasurement())
                    {
                        await action();
                    }
                }
            }
        }
    }
}
