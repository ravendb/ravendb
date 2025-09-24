using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_24349 : RavenTestBase
{
    public RavenDB_24349(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void DynamicFieldsShouldBeMarked()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Dto { Name = "Name1", Type = "Type1" });
                session.Store(new Dto { Name = "Name2", Type = "Type2" });
                
                session.SaveChanges();
            }

            var index = new DummyIndex();
            index.Execute(store);
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var results = session.Query<Dto, DummyIndex>().Where(x => x.Name == "Name1").ToList();
                
                Assert.Equal(1, results.Count);
            }
        }
    }

    private class Dto
    {
        public string Name { get; set; }
        public string Type { get; set; }
    }

    private class DummyIndex : AbstractJavaScriptIndexCreationTask
    {
        public DummyIndex()
        {
            Maps = new HashSet<string>() { "map('Dtos', (dto) => mapDto(dto))" };
            AdditionalSources = new Dictionary<string, string>() { { "Helper", 
                """
                function mapDto(dto) {
                  return {
                    Name: dto.Name, 
                    Type: dto.Type
                  }
                }
                """ } };
        }
    }
}
