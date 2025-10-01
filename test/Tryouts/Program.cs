using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using RachisTests;
using Raven.Server.Utils;
using SlowTests.Issues;
using Tests.Infrastructure;
using Xunit;

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

        for (int i = 0; i < 1000; i++)
            try
            {
                Console.WriteLine(i);
                using (ConsoleTestOutputHelper testOutputHelper = new())
                using (RavenDB_13293 test = new(testOutputHelper))
                {
                    DebuggerAttachedTimeout.DisableLongTimespan = true;

                    await test.CanPassNodeTagToRestorePatchOperation();
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e);
                Console.ForegroundColor = ConsoleColor.White;
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
        string p = AppDomain.CurrentDomain.BaseDirectory;
        string dbPath = Path.Combine(p, "Databases");
        if (Directory.Exists(dbPath))
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
