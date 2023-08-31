using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class IndexCorruptionOnSmallBatches : RavenTestBase
{
    public IndexCorruptionOnSmallBatches(ITestOutputHelper output) : base(output)
    {
    }

    private record Item(string Num);

    [Fact]
    public void ShouldNotHappen()
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = rec =>
            {
                rec.Settings["Indexing.Auto.SearchEngineType"]= "Corax";
                rec.Settings["Indexing.Static.SearchEngineType"] = "Corax";
                rec.Settings["Indexing.MapBatchSize"]= "128";
            }
        });

        using (var s = store.OpenSession())
        {
            for (int i = 0; i < 1024; i++)
            {
                s.Store(new Item(i.ToString()));
            }
            s.SaveChanges();
        }

        using (var s = store.OpenSession())
        {
            _ = s.Query<Item>()
                .OrderBy(x => x.Num)
                .Take(50)
                .ToList();
        }
    }
}
