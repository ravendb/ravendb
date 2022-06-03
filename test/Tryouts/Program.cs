using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Revisions;
using Tests.Infrastructure;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            Console.WriteLine(Process.GetCurrentProcess().Id);
            for (int i = 0; i < 10_000; i++)
            {
                Console.WriteLine($"Starting to run {i}");
                try
                {
                    using (var testOutputHelper = new ConsoleTestOutputHelper())
                    //using (var test = new RollingIndexesClusterTests(testOutputHelper))
                    using (var test = new RevisionsTests(testOutputHelper))
                    {
                        //await test.RemoveNodeFromDatabaseGroupWhileRollingDeployment();
                        await test.CanGetRevisionsCountFor(RavenTestBase.Options.ForMode(RavenDatabaseMode.Sharded));
                    }
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
        }
    }
}
