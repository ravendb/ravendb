using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_18864 : RavenTestBase
{
    private const string RavenDumpPath = "RavenDB_18864.53AutoIndexCoraxImportTest.ravendbdump";

    public RavenDB_18864(ITestOutputHelper output) : base(output)
    {
    }

    private static Stream GetDump(string name)
    {
        var assembly = typeof(RavenDB_18864).Assembly;
        return assembly.GetManifestResourceStream("SlowTests.Data." + name);
    }

    [RavenTheory(RavenTestCategory.BackupExportImport | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task CanImportOldAutoMapDefinitionIntoCoraxAndGenerateProperMapping(Options options)
    {
        using var store = GetDocumentStore(options);
        await using (var stream = GetDump(RavenDumpPath))
        {
            var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
        }
        
        Indexes.WaitForIndexing(store);


        {
            using var s = store.OpenSession();
            Assert.Equal(1, s.Advanced.RawQuery<dynamic>("from index 'Auto/@emptyReducedByNameAndSearch(SearchField)AndTestFor' where TestFor == 'Corax'").Count());
        }
        
        {
            using var s = store.OpenSession();
            Assert.Equal(1, s.Advanced.RawQuery<dynamic>("from index 'Auto/@emptyReducedByNameAndSearch(SearchField)AndTestFor' where Name == 'MACIEJ'").Count());
        }
        
        {
            using var s = store.OpenSession();
            Assert.Equal(1, s.Advanced.RawQuery<dynamic>("from index 'Auto/@emptyReducedByNameAndSearch(SearchField)AndTestFor' where search(SearchField, '*secret*')").Count());
        }
    }
}
