using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23792 : RavenTestBase
{
    public RavenDB_23792(ITestOutputHelper output) : base(output)
    {
    }

    public record Item(RavenVector<float> Vector, string Name);

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TestRqlGeneration(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var q1 = session.Advanced.DocumentQuery<Item>()
                    .VectorSearch(x => x.WithField(i=>i.Vector), 
                        x=>x.ForDocument("items/1-A")).ToString();

                Assert.Equal("from 'Items' where vector.search(Vector, embedding.forDoc($p0))", q1);
                
                var q2 = session.Advanced.DocumentQuery<Item>()
                    .VectorSearch(x => x.WithText(i=>i.Name),
                        x =>x.ForDocument("items/1-A"))
                    .ToString();
                Assert.Equal("from 'Items' where vector.search(embedding.text(Name), embedding.forDoc($p0))", q2);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanQueryByDocumentVector(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Item(null,"Apple Pie"), "items/1-A");
                session.Store(new Item(null,"Orange Cake"), "items/2-A");
                session.Store(new Item(null,"Apple Juice"), "items/3-A");
                session.Store(new Item(null,"Red Carpet"), "items/4-A");
                session.SaveChanges();
            }
            using (var session = store.OpenSession())
            {
                var q1 = session.Advanced.DocumentQuery<Item>()
                    .WaitForNonStaleResults()
                    .VectorSearch(x => x.WithText(i=>i.Name),
                        x =>x.ForDocument("items/1-A"))
                    .Take(3)
                    .ToList();
WaitForUserToContinueTheTest(store);
                var appleIdx = q1.FindIndex(x=>x.Name == "Apple Juice");
                var orangeIdx = q1.FindIndex(x=>x.Name == "Orange Cake");

                Assert.True(appleIdx < orangeIdx);
            }
        }
    }
}
