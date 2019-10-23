using System.Collections.Generic;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_579 : RavenTestBase
    {
        public RavenDB_579(ITestOutputHelper output) : base(output)
        {
        }

        private readonly IList<string> _shardNames = new List<string>
        {
            "1",
            "2",
            "3"
        };

        private class Person
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string MiddleName { get; set; }
        }

        //public ShardedDocumentStore CreateStore()
        //{
        //    var store = new ShardedDocumentStore(
        //        new ShardStrategy(
        //            new Dictionary<string, IDocumentStore>
        //            {
        //                {_shardNames[0], GetDocumentStore()},
        //                {_shardNames[1], GetDocumentStore()},
        //                {_shardNames[2], GetDocumentStore()}
        //            }));

        //    store.Initialize();

        //    return store;
        //}

        //protected override void ModifyStore(DocumentStore store)
        //{
        //    store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;

        //    base.ModifyStore(store);
        //}

        [Fact(Skip = "RavenDB-6283")]
        public void OneShardPerSessionStrategy()
        {
            //using (var store = CreateStore())
            //{
            //    using (var session = store.OpenSession())
            //    {
            //        var sessionMetadata = ExtractSessionMetadataFromSession(session);

            //        var expectedShard = _shardNames[sessionMetadata.GetHashCode() % _shardNames.Count];

            //        var entity1 = new Person { Id = "1", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
            //        session.Store(entity1);
            //        var entity2 = new Person { Id = "2", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
            //        session.Store(entity2);
            //        session.SaveChanges();

            //        var entity3 = new Person { Id = "3", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
            //        session.Store(entity3);
            //        var entity4 = new Person { Id = "4", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
            //        session.Store(entity4);
            //        session.SaveChanges();

            //        Assert.Equal(expectedShard + "/1", entity1.Id);
            //        Assert.Equal(expectedShard + "/2", entity2.Id);
            //        Assert.Equal(expectedShard + "/3", entity3.Id);
            //        Assert.Equal(expectedShard + "/4", entity4.Id);
            //    }

            //    using (var session = store.OpenSession())
            //    {
            //        var sessionMetadata = ExtractSessionMetadataFromSession(session);

            //        var expectedShard = _shardNames[sessionMetadata.GetHashCode() % _shardNames.Count];

            //        var entity1 = new Person { Id = "1", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
            //        session.Store(entity1);
            //        var entity2 = new Person { Id = "2", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
            //        session.Store(entity2);
            //        session.SaveChanges();

            //        Assert.Equal(expectedShard + "/1", entity1.Id);
            //        Assert.Equal(expectedShard + "/2", entity2.Id);
            //    }
            //}
        }

        [Fact(Skip = "RavenDB-6283")]
        public void OneShardPerSessionStrategyAsync()
        {
            //using (var store = CreateStore())
            //{
            //    using (var session = store.OpenAsyncSession())
            //    {
            //        var sessionMetadata = ExtractSessionMetadataFromSession(session);

            //        var expectedShard = _shardNames[sessionMetadata.GetHashCode() % _shardNames.Count];

            //        var entity1 = new Person { Id = "1", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
            //        await session.StoreAsync(entity1);
            //        var entity2 = new Person { Id = "2", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
            //        await session.StoreAsync(entity2);
            //        await session.SaveChangesAsync();

            //        var entity3 = new Person { Id = "3", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
            //        await session.StoreAsync(entity3);
            //        var entity4 = new Person { Id = "4", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
            //        await session.StoreAsync(entity4);
            //        await session.SaveChangesAsync();

            //        Assert.Equal(expectedShard + "/1", entity1.Id);
            //        Assert.Equal(expectedShard + "/2", entity2.Id);
            //        Assert.Equal(expectedShard + "/3", entity3.Id);
            //        Assert.Equal(expectedShard + "/4", entity4.Id);
            //    }

            //    using (var session = store.OpenAsyncSession())
            //    {
            //        var sessionMetadata = ExtractSessionMetadataFromSession(session);

            //        var expectedShard = _shardNames[sessionMetadata.GetHashCode() % _shardNames.Count];

            //        var entity1 = new Person { Id = "1", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
            //        await session.StoreAsync(entity1);
            //        var entity2 = new Person { Id = "2", FirstName = "William", MiddleName = "Edgard", LastName = "Smith" };
            //        await session.StoreAsync(entity2);
            //        await session.SaveChangesAsync();

            //        Assert.Equal(expectedShard + "/1", entity1.Id);
            //        Assert.Equal(expectedShard + "/2", entity2.Id);
            //    }
            //}
        }

        // TODO: Refactor this function when the feature is available
        private object ExtractSessionMetadataFromSession(object session)
        {
            return session;
        }
    }
}
