using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using FastTests.Blittable;
using FastTests.Client.Subscriptions;
using FastTests.Client;
using FastTests.Server.Documents.Revisions;
using RachisTests;
using SlowTests.Issues;
using SlowTests.MailingList;
using SlowTests.Rolling;
using SlowTests.Server.Documents.ETL.Raven;
using StressTests.Issues;
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
        
        public static async Task Main_JSBenchmarking(string[] args)
        {
            var jsEngineTypes = new String[] {"V8", "Jint"};
            var testNames = new String[] {"Simple_Patch_1M"}; //, "Simple_Patch_Put_1M"}; //, "Simple_Patch_Delete_1M"};
            var aggModes = new String[] {"min", "avg", "max"};

            int ratio = 10_000;

            int recordCount = 1000;
            var runsCount = 5;

            Stopwatch sw;
            var results = new Dictionary<string, Dictionary<string, TimeSpan[]>>();
            using (var testOutputHelper = new ConsoleTestOutputHelper())
            using (var test = new BenchmarkTests.Patching.Patch(testOutputHelper))
            {
                for (int i = 0; i < runsCount; i++)
                {
                    Console.WriteLine($"-------------------- run {i+1} --------------------");

                    Console.WriteLine($"------ init dbs ------");
                    foreach (var jsEngineType in jsEngineTypes)
                    {
                        var dbNamePostfix =  $"_{jsEngineType}";
                        sw = Stopwatch.StartNew();
                        using (var store = test.GetSimpleDocumentStore(null, false))
                        {
                            await test.InitAsync(store, dbNamePostfix: dbNamePostfix, options: RavenTestBase.Options.ForJavaScriptEngine(jsEngineType), count: recordCount);
                        }
                        Console.WriteLine($"{jsEngineType}: {sw.Elapsed}");
                    }

                    foreach (var jsEngineType in jsEngineTypes)
                    {
                        Console.WriteLine($"------ {jsEngineType} ------");

                        var dbNamePostfix =  $"_{jsEngineType}";
                        if (!results.ContainsKey(jsEngineType))
                            results[jsEngineType] = new Dictionary<string, TimeSpan[]>();
                        var engineResults = results[jsEngineType];
                        

                        foreach (var testName in testNames)
                        {
                            sw = Stopwatch.StartNew();
                            switch (testName)
                            {
                                case "Simple_Patch_Put_1M":
                                    await test.Simple_Patch_Put_1M(dbNamePostfix, count: recordCount);
                                    break;
                                case "Simple_Patch_1M":
                                    await test.Simple_Patch_1M(dbNamePostfix, count: recordCount);
                                    break;
                                case "Simple_Patch_Delete_1M":
                                    await test.Simple_Patch_Delete_1M(dbNamePostfix, count: recordCount);
                                    break;
                                default:
                                    throw new UnsupportedCommandException(testName);
                            }
                            if (!engineResults.ContainsKey(testName))
                                engineResults[testName] = new TimeSpan[runsCount];
                            engineResults[testName][i] = sw.Elapsed;
                            Console.WriteLine($"{jsEngineType}: {testName}: {engineResults[testName][i]}");
                        }
                    }
                    
                    Console.WriteLine($"---------------- agg {i+1} ----------------");
                    var aggResults = new Dictionary<string, Dictionary<string, Dictionary<string, double>>>();
                    foreach (var testName in testNames)
                    {
                        Console.WriteLine($"------ {testName} ------");
                        foreach (var jsEngineType in jsEngineTypes)
                        {
                            if (!aggResults.ContainsKey(jsEngineType))
                                aggResults[jsEngineType] = new Dictionary<string, Dictionary<string, double>>();
                            var aggEngineResults = aggResults[jsEngineType];
                            var engineResults = results[jsEngineType];

                            if (!aggEngineResults.ContainsKey(testName))
                                aggEngineResults[testName] = new Dictionary<string, double>();
                            var aggTestResults = aggEngineResults[testName];
                            var testResults = engineResults[testName];

                            foreach (var aggMode in aggModes)
                            {
                                aggTestResults[aggMode] = (aggMode) switch
                                {
                                    "min" => testResults.Select(x=>x.TotalSeconds).Min() * ratio / recordCount,
                                    "max" => testResults.Select(x=>x.TotalSeconds).Max() * ratio / recordCount,
                                    "avg" => testResults.Select(x=>x.TotalSeconds).Average() * ratio / recordCount,
                                    _ => throw new UnsupportedOperationException(aggMode)
                                };
                            }
                            Console.WriteLine($"{jsEngineType}:     {aggTestResults[aggModes[0]]} / {aggTestResults[aggModes[1]]} / {aggTestResults[aggModes[2]]}");
                        }
                    }
                }
            }
        }
    }
}
