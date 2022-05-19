using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using FastTests.Client.Subscriptions;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static async Task Main(string[] args)
    {
        var thread = new List<Task>();
        Console.WriteLine(Process.GetCurrentProcess().Id);
        for (int i = 0; i < 100; i++)
        {
            Console.WriteLine($"Starting to run {i}");
            try
            {
                using (var testOutputHelper = new ConsoleTestOutputHelper())
                    //using (var test = new RollingIndexesClusterTests(testOutputHelper))
                using (var test = new SubscriptionsBasic(testOutputHelper))
                {
                    //await test.RemoveNodeFromDatabaseGroupWhileRollingDeployment();
                    thread.Add(
                        Task.Factory.StartNew(() => {
                        using var test = new SlowTests.Tests.Spatial.SpatialSearch(testOutputHelper);
                        test.Can_do_spatial_search_with_client_api_addorder(RavenTestBase.Options.ForSearchEngine(RavenSearchEngineMode.Corax));
                    })
                            );
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e);
                Console.ForegroundColor = ConsoleColor.White;
            }
        }

        Task.WaitAll(thread.ToArray());
    }
}
