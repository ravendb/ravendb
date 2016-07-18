using Microsoft.Xunit.Performance;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Regression
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
                foreach (var iteration in Benchmark.Iterations)
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
