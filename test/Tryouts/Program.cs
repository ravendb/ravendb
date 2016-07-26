using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using AsyncFriendlyStackTrace;
using Raven.Client.Document;
using Raven.Client.Smuggler;
using Raven.Server.ServerWide.Context;
using Raven.SlowTests.Issues;
using Sparrow.Json;

namespace Tryouts
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            var sp = Stopwatch.StartNew();

            var unmanagedBuffersPool = new UnmanagedBuffersPool("test");

            Parallel.For(0, 100 * 1000 * 1000, new ParallelOptions
            {
                MaxDegreeOfParallelism = 150
            }, i =>
            {
                using (var context = new DocumentsOperationContext(unmanagedBuffersPool, null))
                {
                    context.Allocator.Allocate(128);
                }
            });

            Console.WriteLine(sp.ElapsedMilliseconds);
        }
    }
}