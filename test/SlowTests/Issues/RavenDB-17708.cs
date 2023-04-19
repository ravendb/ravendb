using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17708 : RavenTestBase
{
    public RavenDB_17708(ITestOutputHelper output) : base(output)
    {
        
    }
    
    [Fact]
    public async Task ProjectionOnStoredFieldsInIndexWithMixedPropertyProjection()
    {
        using var store = base.GetDocumentStore(new Options
        {
            ModifyDocumentStore = s => s.Conventions.FindProjectedPropertyNameForIndex =
                (indexedType, indexedName, path, prop) =>
                {
                    Func<Type, string, string, string, string> getNameForProjection = prop switch
                    {
                        "Details.Value" => (indexedType, indexedName, path, prop) => (path + prop),
                        _ => DocumentConventions.DefaultFindPropertyNameForIndex
                    };

                    return getNameForProjection(indexedType, indexedName, path, prop);
                }
        });
        
        var id = "myEntity/1";
        var value = 100;

        var e = new MyEntity { Id = id, Name = "Test", Details = new MyEntity.EntityDetails { Description = "Test Description", Value = value, } };

        using (var s = store.OpenAsyncSession())
        {
            await new MyEntityIndexWithStore().ExecuteAsync(store);
            await s.StoreAsync(e);
            await s.SaveChangesAsync();
        }

        await Indexes.WaitForIndexingAsync(store);
        
        using (var s = store.OpenAsyncSession())
        {
            var r1 = await QueryRawCollection(s);
            
            // Both Details_Description and Details_Value from document, we query raw collection
            Assert.Collection(r1, a =>
            {
                Assert.Equal("Test Description", a.Details_Description);
                Assert.Equal(value, a.Details_Value);
            });
            
            var r2 = await QueryUsingJavaScriptProjection(s);
            
            // Details_Description from index, Details_Value from document, as provided in convention
            Assert.Collection(r2, a =>
            {
                Assert.Equal("Testabc", a.Details_Description);
                Assert.Equal(value, a.Details_Value);
            });
        }
    }

    [Fact]
    public async Task ProjectionOnStoredFieldsInIndexWithPropertyProjection()
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s => s.Conventions.FindProjectedPropertyNameForIndex =
                (indexedType, indexedName, path, prop) =>
                {
                    Func<Type, string, string, string, string> getNameForProjection = prop switch
                    {
                        _ => DocumentConventions.DefaultFindPropertyNameForIndex
                    };

                    return getNameForProjection(indexedType, indexedName, path, prop);
                }
        });
        
        var id = "myEntity/1";
        var value = 100;

        var e = new MyEntity { Id = id, Name = "Test", Details = new MyEntity.EntityDetails { Description = "Test Description", Value = value, } };

        using (var s = store.OpenAsyncSession())
        {
            await new MyEntityIndexWithStore().ExecuteAsync(store);
            await s.StoreAsync(e);
            await s.SaveChangesAsync();
        }

        await Indexes.WaitForIndexingAsync(store);
        
        using (var s = store.OpenAsyncSession())
        {
            var res = await QueryUsingJavaScriptProjection(s);
            
            // Both fields from index
            Assert.Collection(res, a =>
            {
                Assert.Equal("Testabc", a.Details_Description);
                Assert.Equal(2237, a.Details_Value);
            });
        }
    }

    [Fact]
    public void ProjectionOnNonStoredFieldsInIndex()
    {
        using var store = GetDocumentStore();
        {
            using (var session = store.OpenSession())
            {
                var e = new MyEntity { Name = "Test", Details = new MyEntity.EntityDetails { Description = "Test Description", Value = 100, } };

                session.Store(e);
                session.SaveChanges();

                var index = new MyEntityIndexWithoutStore();
                index.Execute(store);

                Indexes.WaitForIndexing(store);
                
                var r3 = QueryOnNonStoredFieldsInIndex(session);
                Assert.Collection(r3, a =>
                {
                    Assert.Equal(default, a.Details_Description);
                    Assert.Equal(default, a.Details_Value);
                });
            }
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
        var q = s.Query<MyEntity, MyEntityIndexWithStore>();
        var p = from r in q
            let dummyUselessLoadJustToMakeItWork = RavenQuery.Load<object>("none")
            select new MyEntityDto { Id = r.Id, Name = r.Name, Details_Description = r.Details.Description, Details_Value = r.Details.Value };
        return await p.ToArrayAsync();
    }

    private MyEntityDto[] QueryOnNonStoredFieldsInIndex(IDocumentSession s)
    {
        var q = s.Query<MyEntity, MyEntityIndexWithoutStore>();
        var p = from r in q
            select new MyEntityDto { Id = r.Id, Name = r.Name, Details_Description = r.Details.Description, Details_Value = r.Details.Value };
        return p.ToArray();
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

    private class MyEntityIndexWithoutStore : AbstractIndexCreationTask<MyEntity>
    {
        public MyEntityIndexWithoutStore()
        {
            Map = entities => from e in entities
                select new { Id = e.Id, Search = new object[] { e.Name, e.Details.Description } };

            Index("Search", FieldIndexing.Search);
        }
    }

    private class MyEntityIndexWithStore : AbstractIndexCreationTask<MyEntity>
    {
        public MyEntityIndexWithStore()
        {
            Map = entities => from e in entities
                select new { Id = e.Id, Details_Description = e.Name + "abc", Details_Value = e.Details.Value + 2137 };

            Store("Details_Description", FieldStorage.Yes);
            Store("Details_Value", FieldStorage.Yes);
        }
    }
}
