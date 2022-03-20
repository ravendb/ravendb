using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Raven.Debug.StackTrace;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class BulkInserts : NoDisposalNoOutputNeeded
    {
        public BulkInserts(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Simple_Bulk_Insert_With_Ssl()
        {
            using var p = Process.GetCurrentProcess();
            using (var x = new FastTests.Client.BulkInserts(Output))
            {
                var task = x.Simple_Bulk_Insert(useSsl: true);

                if (task.Wait(TimeSpan.FromSeconds(90)) == false)
                {
                    StringWriter outputWriter = new ();
                    StackTracer.ShowStackTraceWithSnapshot(p.Id, outputWriter);
                    Console.WriteLine("Stack Traces for Simple_Bulk_Insert_With_Ssl");
                    Console.WriteLine(outputWriter.ToString());
                }
                
                await task;
            }
        }
    }
}
