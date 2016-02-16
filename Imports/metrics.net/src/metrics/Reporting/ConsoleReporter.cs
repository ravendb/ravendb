using System;
using System.IO;

namespace metrics.Reporting
{
    
    public class ConsoleReporter : ReporterBase
    {
        public ConsoleReporter(Metrics metrics) : base(Console.Out, metrics)
        {
            
        }

        public ConsoleReporter(IReportFormatter formatter) : base(Console.Out, formatter)
        {

        }
    }
}
