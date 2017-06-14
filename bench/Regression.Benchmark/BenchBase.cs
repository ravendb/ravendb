using System;
using System.Diagnostics;

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
    }
}
