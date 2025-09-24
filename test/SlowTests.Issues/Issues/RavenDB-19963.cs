using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19963 : RavenTestBase
{
    public RavenDB_19963(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void TestSearch()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var rql = session.Query<Dto>()
                    .Search(i => i.Name, "*ion")
                    .Search(i => i.Name, "point", options: SearchOptions.And | SearchOptions.Not)
                    .ToString();
                
                Assert.Equal("from 'Dtos' where search(Name, $p0) and not search(Name, $p1)", rql);

                rql = session.Query<Dto>()
                    .Search(i => i.Name, "*ion")
                    .Search(i => i.Name, "whatever")
                    .Search(i => i.Name, "point", options: SearchOptions.And | SearchOptions.Not)
                    .ToString();
                
                Assert.Equal("from 'Dtos' where (search(Name, $p0) or search(Name, $p1)) and not search(Name, $p2)", rql);
                
                rql = session.Query<Dto>()
                    .Search(i => i.Name, "*ion")
                    .Search(i => i.Name, "whatever", options: SearchOptions.And)
                    .Search(i => i.Name, "point", options: SearchOptions.And | SearchOptions.Not)
                    .ToString();
                
                Assert.Equal("from 'Dtos' where search(Name, $p0) and search(Name, $p1) and not search(Name, $p2)", rql);
                
                rql = session.Query<Dto>()
                    .Search(i => i.Name, "*ion")
                    .Search(i => i.Name, "point", options: SearchOptions.And)
                    .Search(i => i.Name, "point", options: SearchOptions.Or | SearchOptions.Not)
                    .ToString();
                
                Assert.Equal("from 'Dtos' where search(Name, $p0) and search(Name, $p1) or (exists(Name) and not search(Name, $p2))", rql);
                
                rql = session.Query<Dto>()
                    .Search(i => i.Name, "*ion")
                    .Search(i => i.Description, "point", options: SearchOptions.And | SearchOptions.Not)
                    .ToString();

                Assert.Equal("from 'Dtos' where search(Name, $p0) and (exists(Description) and not search(Description, $p1))", rql);
            }
        }
    }

    private class Dto
    {
        public string Name { get; set; }
        
        public string Description { get; set; }
    }
}
