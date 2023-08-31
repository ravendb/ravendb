using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18687 : RavenTestBase
{
    public RavenDB_18687(ITestOutputHelper output) : base(output)
    {
    }

    private const string RavenDumpPath = "RavenDB_18687.auto_indexes.ravendbdump";

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task ShouldNotChangeNameOfAutoIndexes()
    {
        using var store = GetDocumentStore();
        await using (var stream = GetDump(RavenDumpPath))
        {
            var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
        }

        var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));

        Assert.Equal(2, indexes.Length);

        var names = indexes.Select(x => x.Name).ToList();

        Assert.Contains("Auto/Items/By'Name'Andname", names);
        Assert.Contains("Auto/Users/BynameAndName", names);
    }

    private static Stream GetDump(string name)
    {
        var assembly = typeof(RavenDB_18687).Assembly;
        return assembly.GetManifestResourceStream("SlowTests.Data." + name);
    }
}
