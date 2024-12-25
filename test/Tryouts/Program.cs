using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Raven.Server.Utils;
using SlowTests.Corax;
using SlowTests.Sharding.Cluster;
using Xunit;
using FastTests.Voron.Util;
using FastTests.Sparrow;
using FastTests.Voron.FixedSize;
using FastTests.Client.Indexing;
using FastTests;
using Sparrow.Server.Platform;
using SlowTests.Authentication;
using SlowTests.Issues;
using SlowTests.Server.Documents.PeriodicBackup;
using Microsoft.Diagnostics.Tracing.Parsers.MicrosoftAntimalwareEngine;
using NLog;
using RachisTests;
using SlowTests.SlowTests.MailingList;
using FastTests.Issues;
using Voron;
using System.Threading;

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

        using var env = new StorageEnvironment(
            StorageEnvironmentOptions.ForPathForTests(@"F:\ravendb-7.0\test\SlowTests\bin\Debug\net8.0\Databases\GetNewServer-.0-3\Databases\restored_database-39dc4f36-a127-4eda-a8ea-03cb40849c68\Indexes\UsersCountersMapReduceIndex"));
        // for (int i = 0; i < 1000; i++)
        // {
        //     try
        //     {
        //         Console.WriteLine(i);
        //         using (var testOutputHelper = new ConsoleTestOutputHelper())
        //         using (var test = new RavenDB_7940(testOutputHelper))
        //         {
        //             DebuggerAttachedTimeout.DisableLongTimespan = true;
        //
        //             await test.RecreatingIndexesToARecreatedDatabase();
        //         }
        //     }
        //     catch (Exception e)
        //     {
        //         Console.ForegroundColor = ConsoleColor.Red;
        //         Console.WriteLine(e);
        //         Console.ForegroundColor = ConsoleColor.White;
        //     }
        // }
    }

    private static void TryRemoveDatabasesFolder()
    {
        var p = System.AppDomain.CurrentDomain.BaseDirectory;
        var dbPath = Path.Combine(p, "Databases");
        if (Directory.Exists(dbPath))
        {
            try
            {
                Directory.Delete(dbPath, true);
                Assert.False(Directory.Exists(dbPath), "Directory.Exists(dbPath)");
            }
            catch
            {
                Console.WriteLine($"Could not remove Databases folder on path '{dbPath}'");
            }
        }
    }
}
