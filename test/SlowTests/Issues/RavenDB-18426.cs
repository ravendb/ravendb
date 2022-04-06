using FastTests;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes.Static.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18426 : RavenTestBase
{
    public RavenDB_18426(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void ProjectionIdTest()
    {
        using (var store = GetDocumentStore())
        {
            new DocIndex().Execute(store);
            new DocReduceIndex().Execute(store);
            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = "doc-1", IntVal = 1 });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                session.Query<DocView, DocIndex>()
                    .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
                    .ProjectInto<DocView>()
                    .ToList(); // should not throw - map

                session.Query<DocView, DocReduceIndex>()
                    .Customize(x => x.Projection(ProjectionBehavior.FromIndexOrThrow))
                    .ProjectInto<DocView>()
                    .ToList(); // should not throw - map/reduce

                
            }
        }
    }

    class Doc
    {
        public string Id { get; set; }
        public int? IntVal { get; set; }
    }

    class DocView
    {
        public string Id { get; set; }
    }

    class DocIndex : AbstractIndexCreationTask<Doc>
    {
        public DocIndex()
        {
            Map = docs =>
                from doc in docs
                select new { Id = doc.Id, IntVal = doc.IntVal, };
            StoreAllFields(FieldStorage.Yes);
            Store(x => x.Id, FieldStorage.Yes);
        }
    }
    
     class DocReduceIndex : AbstractIndexCreationTask<Doc>
    {
        public DocReduceIndex()
        {
            Map = docs =>
                from doc in docs
                select new
                {
                    Id = doc.Id,
                    IntVal = doc.IntVal,
                };
            Reduce = results =>
                from result in results
                group result by result.Id into g
                select new
                {
                    Id = g.First().Id,
                    IntVal = g.First().IntVal,
                };
            StoreAllFields(FieldStorage.Yes);
        }
    }
}
