using FastTests;
using Newtonsoft.Json.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20972 : RavenTestBase
{
    public RavenDB_20972(ITestOutputHelper output) : base(output)
    {
    }

    private record Item(string Name, string Parent);

    [RavenFact(RavenTestCategory.Querying)]
    public void CanProjectIdInNestedDocuments()
    {
        using var store = GetDocumentStore();

        using (var s = store.OpenSession())
        {
            s.Store(new Item("Box", null), "items/1");
            s.Store(new Item("Bottle", "items/1"), "items/2");
            s.SaveChanges();
        }

        using (var s = store.OpenSession())
        {
            var result = s.Advanced.RawQuery<JObject>("from Items as i where id(i) == 'items/2' load i.Parent as p select p, i")
                .Single();
            Assert.True(result.Value<JObject>("p").Value<JObject>("@metadata").ContainsKey("@id"));
            Assert.True(result.Value<JObject>("i").Value<JObject>("@metadata").ContainsKey("@id"));
        }
        
        using (var s = store.OpenSession())
        {
            var result = s.Advanced.RawQuery<JObject>("from Items as i where id(i) == 'items/2' load i.Parent as p select { p, i }")
                .Single();
            Assert.True(result.Value<JObject>("p").Value<JObject>("@metadata").ContainsKey("@id"));
            Assert.True(result.Value<JObject>("i").Value<JObject>("@metadata").ContainsKey("@id"));
        }
    }
    
}
