#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
using System;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Raven.Server.Utils;
using Xunit;
using FastTests;
using Raven.Client.Documents.Operations.AI;
using SlowTests.Server.Documents.AI;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        // XunitLogging removed in xUnit v3 migration
    }

    public static async Task Main(string[] args)
    {
        Console.WriteLine(Process.GetCurrentProcess().Id);
        var sources = EventSource.GetSources();
        var runtime = sources.FirstOrDefault(x => x.Name == "System.Runtime");
        runtime?.Dispose();
        for (int i = 0; i < 1; i++)
        {
            Console.WriteLine($"Starting to run {i}");
            
            try
            {
                using (var testOutputHelper = new ConsoleTestOutputHelper())
                await using (var test = new ChatCompletionClientTests(testOutputHelper))
                {
                    DebuggerAttachedTimeout.DisableLongTimespan = true;
                    var p = GetGenAiConfig(RavenAiIntegration.OpenAi);
                    await test.GenAiClientSanityTest(p.Options, p.Configuration);
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

    private static (RavenTestBase.Options Options, GenAiConfiguration Configuration) GetGenAiConfig(RavenAiIntegration type, RavenDatabaseMode databaseMode = RavenDatabaseMode.Single)
    {
        var att = new RavenGenAiDataAttribute();
        var connector = att.GetAiConnectionStringsSingleton(type).First();
        var config = connector.GetAiConfiguration();
        var options = RavenTestBase.Options.ForMode(databaseMode);
        return (options, config);
    }

    private static (RavenTestBase.Options Options, EmbeddingsGenerationConfiguration Configuration) GetEmbeddingsConfig(RavenAiIntegration type, RavenDatabaseMode databaseMode = RavenDatabaseMode.Single)
    {
        var att = new RavenAiEmbeddingsDataAttribute();
        var connector = att.GetAiConnectionStringsSingleton(type).First();
        var config = connector.GetAiConfiguration();
        var options = RavenTestBase.Options.ForMode(databaseMode);
        return (options, config);
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
