using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Server.Documents.Indexes.Static.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class GH_15634 : RavenTestBase
{
    public GH_15634(ITestOutputHelper output) : base(output)
    {
    }
    
    
    [Fact]
    public async Task CanProjectFromProjectInto()
    {
        using var store = GetDocumentStore();
        store.ExecuteIndex(new EntityBaseIndex());

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Asset { Id = "Asset/d07bba18-685f-4eb8-b974-ed2ab5aa1ff5", Tags = new() { "tag1", "tag2" } });
            await session.StoreAsync(new Asset { Id = "Asset/bca893c9-c8c6-4913-b373-df2547aa128a", Tags = new() { "tag2" } });
            await session.StoreAsync(new Asset { Id = "Asset/d322936f-66e0-463f-8cda-cf5031b97d8d", Tags = new() { "tag1" } });
            await session.StoreAsync(new Asset { Id = "Asset/d2646c1f-edee-44fc-b173-093589493726", Tags = new() { "tag1", "tag2" } });
            await session.SaveChangesAsync();
        }
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenAsyncSession())
        {
            var assignableTypeNames = new[] { "Asset" };
            var tags = new[] { "tag1", "tag2" };

            // this query works using RavenDb.Client <=5.4.2
            // from version 5.4.3, it fails with a JsonSerializationException
            var ravenQueryable = session.Query<EntityBaseResult>("EntityBaseIndex")
                .ProjectInto<EntityBaseResult>()
                .Where(d => d.ModelType.In(assignableTypeNames))
                .Where(a => a.Tags.ContainsAll(tags))
                .Select(a => a.Tags_Count );
            var results = await ravenQueryable.ToListAsync();
            Assert.NotEmpty(results);
            Assert.Equal(2, results[0]);
        }
    }
    class EntityBaseIndex : AbstractIndexCreationTask<Asset>
    {
        public EntityBaseIndex()
        {
            Map = assets => from asset in assets
                select new EntityBaseResult
                {
                    DatabaseId = asset.Id,
                    ModelType = asset.ModelType,
                    Tags = asset.Tags,
                    Tags_Count = asset.Tags.Count,
                };

            StoreAllFields(FieldStorage.Yes);
        }
    }

    class Asset
    {
        public string Id { get; set; } = "";
        public HashSet<string> Tags { get; set; } = new();
        public string ModelType => GetType().Name;
    }

    class EntityBaseResult
    {
        public string DatabaseId { get; set; } = "";
        public string ModelType { get; set; } = "";
        public IEnumerable<string> Tags { get; set; } = default!;
        public int Tags_Count { get; set; }
    }
}
