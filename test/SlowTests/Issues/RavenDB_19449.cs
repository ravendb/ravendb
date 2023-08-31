#nullable enable
using System.IO;
using System.IO.Compression;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Utils;
using SlowTests.Corax;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19449 : RavenTestBase
{
    public RavenDB_19449(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void BackwardCompatibilityForEntriesCountWrittenAsInt()
    {
        var serverPath = NewDataPath();
        var databasePath = NewDataPath();
        DeleteAndExtractFiles(databasePath, $"{nameof(RavenDB_19449)}.zip");
        using (var server = GetNewServer(new ServerCreationOptions { DataDirectory = serverPath, RunInMemory = false }))
        using (var store = GetDocumentStore(new Options {Server = server, RunInMemory = false, Path = databasePath, ModifyDatabaseName = _ => "Test"}))
        {
            var stat = store.Maintenance.Send(new GetIndexStatisticsOperation("TestIndex"));
            Assert.NotNull(stat);
            Assert.NotEqual(IndexState.Error, stat.State);
        }
    }
    
    private static void DeleteAndExtractFiles(string destination, string zipPathInAssembly)
    {
        using (var stream = GetFile(zipPathInAssembly))
        using (var archive = new ZipArchive(stream!))
        {
            IOExtensions.DeleteDirectory(destination);
            archive.ExtractToDirectory(destination);
        }
    }
    
    private static Stream? GetFile(string name)
    {
        var assembly = typeof(IndexBackwardCompatibilityAndPersistence).Assembly;
        return assembly.GetManifestResourceStream($"SlowTests.Data.{nameof(RavenDB_19449)}." + name);
    }
}
