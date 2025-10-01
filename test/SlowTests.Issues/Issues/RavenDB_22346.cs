using System;
using Raven.Client.Documents.Indexes;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22346 : RavenTestBase
{
    public RavenDB_22346(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CoraxMultipleOrder_SearchEngine_Test2()
    {
        using var store = GetDocumentStore();

        store.ExecuteIndex(new OrderByIndexCorax());

        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 100; i++)
            {
                session.Store(new TestDocument { Name = "Name_" + i.ToString(), DateCreated = new DateTime(2000, 1, 1).AddDays(i), Archived = i % 2 == 0 });
            }

            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var page1 = session.Query<TestDocument, OrderByIndexCorax>()
                .OrderBy(x => x.Archived).ThenByDescending(x => x.DateCreated).Skip(0).Take(10)
                .ToList();

            var page2 = session.Query<TestDocument, OrderByIndexCorax>()
                .OrderBy(x => x.Archived).ThenByDescending(x => x.DateCreated).Skip(10).Take(10)
                .ToList();

            Assert.NotEqual(page1.First().Id, page2.First().Id);
        }
    }
    
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void CoraxMultipleOrder_SearchEngine_Test()
    {
        using var store = GetDocumentStore();
        store.ExecuteIndex(new OrderByIndexLucene());
        store.ExecuteIndex(new OrderByIndexCorax());
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < 100; i++)
            {
                var obj = new TestDocument { Name = "Name_" + i.ToString(), DateCreated = new DateTime(2000, 1, 1).AddDays(i), Archived = i % 2 == 0 };
                session.Store(obj);
            }

            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var luceneDocs = session.Query<TestDocument, OrderByIndexLucene>()
                .OrderBy(x => x.Archived)
                .ThenByDescending(x => x.DateCreated)
                .Skip(10)
                .Take(10)
                .Select(x => x.Id)
                .ToList();

            var coraxDocs = session.Query<TestDocument, OrderByIndexCorax>()
                .OrderBy(x => x.Archived).ThenByDescending(x => x.DateCreated).Skip(10).Take(10)
                .Select(x => x.Id)
                .ToList();

            Assert.Equal(luceneDocs, coraxDocs);

        }

    }
    
    private class OrderByIndexLucene : AbstractIndexCreationTask<TestDocument>
    {
        public OrderByIndexLucene()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;

            Map = entities => from entity in entities
                select new
                {
                    entity.Id,
                    entity.Name,
                    entity.Archived,
                    entity.DateCreated
                };

        }
    }
    
    private class OrderByIndexCorax : AbstractIndexCreationTask<TestDocument>
    {
        public OrderByIndexCorax()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;

            Map = entities => from entity in entities
                select new { entity.Id, entity.Name, entity.Archived, entity.DateCreated };
        }
    }

    private class TestDocument
    {
        public string Id { get; set; } = null!;
        public bool Archived { get; set; }
        public DateTime DateCreated { get; set; }
        public string Name { get; set; } = null!;
    }
}
