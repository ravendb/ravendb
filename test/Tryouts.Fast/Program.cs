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
using FastTests.Client;
using Raven.Client.Documents.Operations.AI;

namespace Tryouts.Fast;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
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
                using (var test = new CRUD(testOutputHelper))
                {
                    DebuggerAttachedTimeout.DisableLongTimespan = true;
                    test.CRUD_Operations(RavenTestBase.Options.ForMode(RavenDatabaseMode.Single), true);
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
        var connector = att.GetAiConnectionStringsNewInstance(type, "").First();
        var config = connector.GetAiConfiguration();
        var options = RavenTestBase.Options.ForMode(databaseMode);
        return (options, config);
    }

    private static (RavenTestBase.Options Options, EmbeddingsGenerationConfiguration Configuration) GetEmbeddingsConfig(RavenAiIntegration type, RavenDatabaseMode databaseMode = RavenDatabaseMode.Single)
    {
        var att = new RavenAiEmbeddingsDataAttribute();
        var connector = att.GetAiConnectionStringsNewInstance(type, "").First();
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
