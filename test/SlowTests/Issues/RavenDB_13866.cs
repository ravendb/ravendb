using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Issues
{
    public class RavenDB_13866 : RavenTestBase
    {
        public RavenDB_13866(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task MapAndReduceIndexingOutputMustNotShareTheSamePropertyAccessorCache()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new TestIndex());

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Tag {Id = "tags/1", IsDefault = true});
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    ////***** IF YOU REMOVE THIS LINE, THE TEST PASSES!!!  *****////
                    var tag1 = await session.LoadAsync<Tag>("tags/1");
                    Assert.NotNull(tag1);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Entity {Id = "docs/1", TagIds = new List<string> {"tags/1"}});
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var tag1 = await session
                        .Query<TestIndex.Result, TestIndex>()
                        .SingleAsync(t => t.TagId == "tags/1");

                    Assert.Equal(1, tag1.Count);
                }
            }
        }

        public class TestIndex : AbstractMultiMapIndexCreationTask<TestIndex.Result>
        {
            public class Result
            {
                public string TagId { get; set; }
                public bool IsDefault { get; set; }
                public int Count { get; set; }
            }

            public TestIndex()
            {
                AddMap<Tag>(tags => from tag in tags
                    select new {TagId = tag.Id, IsDefault = tag.IsDefault, Count = 0});

                AddMap<Entity>(entities => from entity in entities
                    from tagId in entity.TagIds
                    select new {TagId = tagId, IsDefault = false, Count = 1});

                Reduce = results => from result in results
                    group result by result.TagId
                    into groupedByTag
                    select new {TagId = groupedByTag.Key, IsDefault = groupedByTag.Any(t => t.IsDefault), Count = groupedByTag.Sum(t => t.Count)};
            }
        }

        public class Tag
        {
            public string Id { get; set; }
            public bool IsDefault { get; set; }
        }

        public class Entity
        {
            public string Id { get; set; }
            public List<string> TagIds { get; set; }
        }
    }
}
