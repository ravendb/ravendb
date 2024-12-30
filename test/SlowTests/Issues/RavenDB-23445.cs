using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23445 : RavenTestBase
{
    public RavenDB_23445(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void TestIndexingOfNulls(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var territory1 = new Territory() { Name = "New York" };
                var territory2 = new Territory() { Name = null };
                
                var dto1 = new Dto() { Territories = new List<Territory>() { territory1, territory2 } };
                var dto2 = new Dto() { Territories = new List<Territory>() { territory2, null } };
                
                session.Store(dto1);
                session.Store(dto2);
                
                session.SaveChanges();
                
                var results = session.Advanced.RawQuery<Dto>("from \"Dtos\" where vector.search(embedding.text(Territories[].Name), \"New York\")").ToList();
                
                Indexes.WaitForIndexing(store);
                
                Assert.Equal(1, results.Count);
            }
        }
    }

    private class Dto
    {
        public List<Territory> Territories { get; set; }
    }

    private class Territory
    {
        public string Name { get; set; }
    }
}
