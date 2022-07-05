using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using FastTests.Client.Subscriptions;
using FastTests.Voron;
using SlowTests.Voron.CompactTrees;
using FastTests.Voron;
using FastTests.Voron.Sets;
using FastTests.Corax.Bugs;
using RachisTests.DatabaseCluster;
using Raven.Server.Utils;
using SlowTests.Cluster;
using SlowTests.Issues;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    public static async Task Main(string[] args)
    {
        Console.WriteLine(Process.GetCurrentProcess().Id);
        for (int i = 0; i < 100; i++)
        {
            Console.WriteLine($"Starting to run {i}");
            try
            {
                using (var testOutputHelper = new ConsoleTestOutputHelper())
                using (var test = new ClusterDatabaseMaintenance(testOutputHelper))
                {
                    DebuggerAttachedTimeout.DisableLongTimespan = true;
                    await test.OnlyOneNodeShouldUpdateRehab();
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e);
                Console.ForegroundColor = ConsoleColor.White;
                return;
            }
        }
    }
}
