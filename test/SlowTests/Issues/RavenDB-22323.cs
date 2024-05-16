using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22323 : RavenTestBase
{
    public RavenDB_22323(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void TestParenthesesWhenUsingSearchAndWhereClause()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var query = session.Query<DummyIndex.Result, DummyIndex>()
                    .Where(d => d.Category == "A" || d.Category == "B")
                    .Search(d => d.Name, "Peter*")
                    .ToString();

                Assert.Equal("from index 'DummyIndex' where (Category = $p0 or Category = $p1) and search(Name, $p2)", query);

                query = session.Query<DummyIndex.Result, DummyIndex>()
                    .Search(d => d.Name, "Peter*")
                    .Where(d => d.Category == "A" || d.Category == "B")
                    .ToString();

                Assert.Equal("from index 'DummyIndex' where search(Name, $p0) and (Category = $p1 or Category = $p2)", query);

                query = session.Query<DummyIndex.Result, DummyIndex>()
                    .Where(d => d.Category == "A" || d.Category == "B")
                    .ToString();

                Assert.Equal("from index 'DummyIndex' where Category = $p0 or Category = $p1", query);

                query = session.Query<DummyIndex.Result, DummyIndex>()
                    .Where(d => d.Category == "A")
                    .ToString();

                Assert.Equal("from index 'DummyIndex' where Category = $p0", query);

                query = session.Query<DummyIndex.Result, DummyIndex>()
                    .Where(d => d.Category == "A")
                    .Where(d => d.Name == "aaa")
                    .ToString();

                Assert.Equal("from index 'DummyIndex' where (Category = $p0) and (Name = $p1)", query);

                query = session.Query<DummyIndex.Result, DummyIndex>()
                    .Where(d => d.Category == "A")
                    .Search(d => d.Name, "Peter*")
                    .ToString();

                Assert.Equal("from index 'DummyIndex' where (Category = $p0) and search(Name, $p1)", query);

                query = session.Query<DummyIndex.Result, DummyIndex>()
                    .Search(d => d.Name, "Peter*")
                    .Search(d => d.Category, "A", options: SearchOptions.Or)
                    .Where(d => d.Category == "A" || d.Category == "B")
                    .Search(d => d.Surname, "DDD", options: SearchOptions.And)
                    .ToString();

                Assert.Equal("from index 'DummyIndex' where search(Name, $p0) or search(Category, $p1) and (Category = $p2 or Category = $p3) and search(Surname, $p4)", query);

                query = session.Query<DummyIndex.Result, DummyIndex>()
                    .Where(d => d.Category == "A" || d.Category == "B")
                    .Search(d => d.Name, "Peter*")
                    .Search(d => d.Category, "A", options: SearchOptions.Or)
                    .Search(d => d.Surname, "DDD", options: SearchOptions.And)
                    .ToString();

                Assert.Equal("from index 'DummyIndex' where (Category = $p0 or Category = $p1) and search(Name, $p2) or search(Category, $p3) and search(Surname, $p4)", query);

                query = session.Query<DummyIndex.Result, DummyIndex>()
                    .Search(d => d.Name, "Peter*")
                    .Where(d => d.Category == "A")
                    .Search(d => d.Category, "A")
                    .ToString();

                Assert.Equal("from index 'DummyIndex' where search(Name, $p0) and (Category = $p1) and search(Category, $p2)", query);

                query = session.Query<DummyIndex.Result, DummyIndex>()
                    .Where(d => d.Name == "Peter")
                    .Search(d => d.Name, "Peter*")
                    .Where(d => d.Category == "A")
                    .ToString();

                Assert.Equal("from index 'DummyIndex' where (Name = $p0) and search(Name, $p1) and (Category = $p2)", query);
            }
        }
    }

    private class Dto
    { 
        public string Category { get; set; }
        public string Name { get; set; }
        public string Surname { get; set; }
    }

    private class DummyIndex : AbstractIndexCreationTask<Dto>
    {
        public class Result
        {
            public string Category { get; set; }
            public string Name { get; set; }
            public string Surname { get; set; }
        }
        
        public DummyIndex()
        {
            Map = dtos => from dto in dtos
                select new Result()
                {
                    Category = dto.Category, 
                    Name = dto.Name, 
                    Surname = dto.Surname
                };
        }
    }
}
