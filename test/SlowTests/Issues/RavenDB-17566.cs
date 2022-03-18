using System;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Issues;

public class RavenDB_17566 : RavenTestBase
{
    public RavenDB_17566(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task ProjectionOnNonStoredFieldsInIndex()
    {
        var myEntityIndex = new MyEntityIndex();
        using var store = base.GetDocumentStore(new Options
        {
            ModifyDocumentStore = s => s.Conventions.FindProjectedPropertyNameForIndex =
                (indexedType, indexedName, path, prop) =>
                {
                    if (indexedName == myEntityIndex.IndexName)
                        return path + prop;

                    return DocumentConventions.DefaultFindPropertyNameForIndex(indexedType, indexedName, path, prop);
                }
        });
        
        var id = "myEntity/1";
        var value = 100;

        var e = new MyEntity { Id = id, Name = "Test", Details = new MyEntity.EntityDetails { Description = "Test Description", Value = value, } };

        using (var s = store.OpenAsyncSession())
        {
            await new MyEntityIndex().ExecuteAsync(store);
            await s.StoreAsync(e);
            await s.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        using (var s = store.OpenAsyncSession())
        {
            var r1 = await QueryRawCollection(s);
            Assert.Collection(r1, a => Assert.Equal(value, a.Details_Value));

            var r2 = await QueryUsingJavaScriptProjection(s);
            Assert.Collection(r2, a => Assert.Equal(value, a.Details_Value));

            var r3 = await QueryOnNonStoredFieldsInIndex(s);
            Assert.Collection(r3, a => Assert.Equal(value, a.Details_Value));
        }
    }

    private async Task<MyEntityDto[]> QueryRawCollection(IAsyncDocumentSession s)
    {
        var q = s.Query<MyEntity>()
            .Select(r => new MyEntityDto { Id = r.Id, Name = r.Name, Details_Description = r.Details.Description, Details_Value = r.Details.Value });
        return await q.ToArrayAsync();
    }

    private async Task<MyEntityDto[]> QueryUsingJavaScriptProjection(IAsyncDocumentSession s)
    {
        var q = s.Query<MyEntity, MyEntityIndex>();
        var p = from r in q
            let dummyUselessLoadJustToMakeItWork = RavenQuery.Load<object>("none")
            select new MyEntityDto { Id = r.Id, Name = r.Name, Details_Description = r.Details.Description, Details_Value = r.Details.Value };
        return await p.ToArrayAsync();
    }

    private async Task<MyEntityDto[]> QueryOnNonStoredFieldsInIndex(IAsyncDocumentSession s)
    {
        var q = s.Query<MyEntity, MyEntityIndex>();
        var p = from r in q
            select new MyEntityDto { Id = r.Id, Name = r.Name, Details_Description = r.Details.Description, Details_Value = r.Details.Value };
        return await p.ToArrayAsync();
    }

    private class MyEntity
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public EntityDetails Details { get; set; }

        public MyEntity()
        {
            Details = new EntityDetails();
        }

        public class EntityDetails
        {
            public string Description { get; set; }
            public int Value { get; set; }
        }
    }

    private class MyEntityDto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Details_Description { get; set; }
        public int Details_Value { get; set; }
    }

    private class MyEntityIndex : AbstractIndexCreationTask<MyEntity>
    {
        public MyEntityIndex()
        {
            Map = entities => from e in entities
                select new { Id = e.Id, Search = new object[] { e.Name, e.Details.Description } };

            Index("Search", FieldIndexing.Search);
        }
    }
}
