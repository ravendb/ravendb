using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Task = System.Threading.Tasks.Task;
using Web = Raven.Server.Web;

namespace SlowTests.Issues;

public class RavenDB_14963 : RavenTestBase
{
    public RavenDB_14963(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Voron)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = new object[] { true })]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = new object[] { false })]
    public async Task TestAllRevisionsView(Options options, bool compression)
    {
        if (compression)
        {
            options.ModifyDatabaseRecord += r =>
            {
                r.DocumentsCompression = new DocumentsCompressionConfiguration
                {
                    CompressAllCollections = true,
                    CompressRevisions = true
                };
            };
        }

        using var store = GetDocumentStore(options);

        var list = await Initialize(store);
        if (options.DatabaseMode == RavenDatabaseMode.Single)
        {
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var usersLastCv = db.DocumentsStorage.RevisionsStorage.GetLastRevisionChangeVectorForCollection(ctx, "Users");
                var ordersLastCv = db.DocumentsStorage.RevisionsStorage.GetLastRevisionChangeVectorForCollection(ctx, "Orders");
                var lastCv = db.DocumentsStorage.RevisionsStorage.GetLastRevisionChangeVector(ctx);

                Assert.Equal(lastCv, ordersLastCv);

                var lastEtag = Convert.ToInt64(lastCv.Split(":")[1].Split("-")[0]);
                var lastUsersEtag = Convert.ToInt64(usersLastCv.Split(":")[1].Split("-")[0]);

                Assert.True(lastEtag > lastUsersEtag, $"lastCv {lastCv}, lastEtag {lastEtag}, usersLastCv: {usersLastCv}, lastUsersEtag {lastUsersEtag}");

            }
        }

        var ct = await AssertResultsAsync(store, list, expectedTotalResults: 12, start: 0, pageSize: 3, continuationToken: null, options.DatabaseMode);
        ct = await AssertResultsAsync(store, list, expectedTotalResults: 12, start: 3, pageSize: 3, continuationToken: ct, options.DatabaseMode);
        ct = await AssertResultsAsync(store, list, expectedTotalResults: 12, start: 6, pageSize: 3, continuationToken: ct, options.DatabaseMode);
        await AssertResultsAsync(store, list, expectedTotalResults: 12, start: 0, pageSize: 8, continuationToken: null, options.DatabaseMode);
        ct = await AssertResultsAsync(store, list, expectedTotalResults: 2, start: 0, pageSize: 4, continuationToken: null, options.DatabaseMode, null, RevisionsType.Deleted);
        await AssertResultsAsync(store, list, expectedTotalResults: 2, start: 4, pageSize: 4, continuationToken: ct, options.DatabaseMode, null, RevisionsType.Deleted);
        ct = await AssertResultsAsync(store, list, expectedTotalResults: 10, start: 0, pageSize: 5, continuationToken: null, options.DatabaseMode, null, RevisionsType.Regular);
        await AssertResultsAsync(store, list, expectedTotalResults: 10, start: 5, pageSize: 10, continuationToken: ct, options.DatabaseMode, null, RevisionsType.Regular);

        await AssertResultsAsync(store, list, expectedTotalResults: 0, start: 0, pageSize: 8, continuationToken: null, options.DatabaseMode, "Companies");
        ct = await AssertResultsAsync(store, list, expectedTotalResults: 0, start: 0, pageSize: 4, continuationToken: null, options.DatabaseMode, "Companies", RevisionsType.Deleted);
        await AssertResultsAsync(store, list, expectedTotalResults: 0, start: 4, pageSize: 4, continuationToken: ct, options.DatabaseMode, "Companies", RevisionsType.Deleted);
        ct = await AssertResultsAsync(store, list, expectedTotalResults: 0, start: 0, pageSize: 4, continuationToken: null, options.DatabaseMode, "Companies", RevisionsType.Regular);
        await AssertResultsAsync(store, list, expectedTotalResults: 0, start: 4, pageSize: 4, continuationToken: ct, options.DatabaseMode, "Companies", RevisionsType.Regular);

        ct = await AssertResultsAsync(store, list, expectedTotalResults: 4, start: 0, pageSize: 3, continuationToken: null, options.DatabaseMode, "Users");
        await AssertResultsAsync(store, list, expectedTotalResults: 4, start: 3, pageSize: 3, continuationToken: ct, options.DatabaseMode, "Users");
        await AssertResultsAsync(store, list, expectedTotalResults: 0, start: 0, pageSize: 3, continuationToken: null, options.DatabaseMode, "Users", RevisionsType.Deleted);
        await AssertResultsAsync(store, list, expectedTotalResults: 0, start: 0, pageSize: 1, continuationToken: null, options.DatabaseMode, "Users", RevisionsType.Deleted);
        await AssertResultsAsync(store, list, expectedTotalResults: 4, start: 0, pageSize: 8, continuationToken: null, options.DatabaseMode, "Users", RevisionsType.Regular);
        await AssertResultsAsync(store, list, expectedTotalResults: 4, start: 0, pageSize: 1, continuationToken: null, options.DatabaseMode, "Users", RevisionsType.Regular);
        await AssertResultsAsync(store, list, expectedTotalResults: 0, start: 0, pageSize: 8, continuationToken: null, options.DatabaseMode, "Users", RevisionsType.Deleted);
        await AssertResultsAsync(store, list, expectedTotalResults: 0, start: 0, pageSize: 1, continuationToken: null, options.DatabaseMode, "Users", RevisionsType.Deleted);
        await AssertResultsAsync(store, list, expectedTotalResults: 4, start: 0, pageSize: 8, continuationToken: null, options.DatabaseMode, "Users", RevisionsType.Regular);
        await AssertResultsAsync(store, list, expectedTotalResults: 4, start: 0, pageSize: 1, continuationToken: null, options.DatabaseMode, "Users", RevisionsType.Regular);

        ct = await AssertResultsAsync(store, list, expectedTotalResults: 4, start: 0, pageSize: 2, continuationToken: null, options.DatabaseMode, "Docs");
        ct = await AssertResultsAsync(store, list, expectedTotalResults: 4, start: 2, pageSize: 2, continuationToken: ct, options.DatabaseMode, "Docs");
        ct = await AssertResultsAsync(store, list, expectedTotalResults: 2, start: 0, pageSize: 1, continuationToken: null, options.DatabaseMode, "Docs", RevisionsType.Deleted);
        await AssertResultsAsync(store, list, expectedTotalResults: 0, start: 1, pageSize: 1, continuationToken: ct, options.DatabaseMode, "Docs", RevisionsType.Deleted);
        ct = await AssertResultsAsync(store, list, expectedTotalResults: 0, start: 0, pageSize: 1, continuationToken: null, options.DatabaseMode, "Docs", RevisionsType.Regular);
        await AssertResultsAsync(store, list, expectedTotalResults: 0, start: 1, pageSize: 1, continuationToken: ct, options.DatabaseMode, "Docs", RevisionsType.Regular);
    }


    private async Task<string> AssertResultsAsync(DocumentStore store, List<(string Id, string Name)> list,
    int expectedTotalResults, int start, int pageSize, string continuationToken, RavenDatabaseMode databaseMode, string collection = null, RevisionsType type = RevisionsType.All)
    {
        RevisionsPreviewResults results = null;
        if (continuationToken != null)
            results = await store.Maintenance.SendAsync(new RevisionsCollectionPreviewOperation(collection, type, continuationToken));
        else
        {
            Assert.False(databaseMode == RavenDatabaseMode.Sharded && start != 0, "must pass continuationToken when start is greater then 0");
            results = await store.Maintenance.SendAsync(new RevisionsCollectionPreviewOperation(collection, type, start, pageSize));
        }

        if (collection == null || type == RevisionsType.All)
            Assert.Equal(expectedTotalResults, results.TotalResults); // There is no "TotalResults" Property on the response json for collection amd filter

        Assert.True(pageSize >= results.Results.Count);

        if (collection != null)
            list = list.Where(info => info.Id.StartsWith(collection)).ToList();

        IEnumerable<(string Id, string Name)> filter;
        switch (type)
        {
            case RevisionsType.Deleted:
                filter = list.Where(x => x.Name == null);
                break;
            case RevisionsType.Regular:
                filter = list.Where(x => x.Name != null);
                break;
            case RevisionsType.All:
                filter = list;
                break;
            default:
                throw new InvalidOperationException($"type {type} is Invalid");
        }

        list = filter.Skip(start).Take(pageSize).ToList();
        Assert.Equal(results.Results.Count, list.Count);

        for (int i = 0; i < list.Count; i++)
        {
            var result = results.Results[i];
            var info = list[i];

            Assert.Equal(info.Id, result.Id);

            if (info.Name == null)
            {
                continue; //Deleted Revision
            }

            string name = null;

            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.Advanced.Revisions.GetAsync<dynamic>(result.ChangeVector);
                name = doc.Name;
            }

            Assert.NotNull(name);
            Assert.Equal(info.Name, name);
        }

        return results.ContinuationToken;
    }


    private async Task<List<(string Id, string Name)>> Initialize(DocumentStore store)
    {
        var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false } };
        await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

        var list = new List<(string Id, string Name)>();

        for (int i = 0; i < 2; i++)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = i.ToString() }, "Users/1");
                list.Add(("Users/1", i.ToString()));
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = i.ToString() }, "Users/2");
                list.Add(("Users/2", i.ToString()));
                await session.SaveChangesAsync();
            }
        }

        for (int i = 0; i < 2; i++)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Doc { Name = i.ToString() }, "Docs/1");
                list.Add(("Docs/1", i.ToString()));
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Doc { Name = i.ToString() }, "Docs/2");
                list.Add(("Docs/2", i.ToString()));
                await session.SaveChangesAsync();
            }
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Order { Name = "0" }, "Order/1");
            list.Add(("Order/1", "0"));
            await session.SaveChangesAsync();

            session.Delete("Order/1");
            list.Add(("Order/1", null));
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Order { Name = "0" }, "Order/2");
            list.Add(("Order/2", "0"));
            await session.SaveChangesAsync();

            session.Delete("Order/2");
            list.Add(("Order/2", null));
            await session.SaveChangesAsync();
        }

        list.Reverse();

        return list;
    }

    [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Voron)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task GetRevisionsIdsByPrefixAndStatsTests(Options options)
    {
        using var store = GetDocumentStore(options);
        var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false } };
        await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

        using (var session = store.OpenAsyncSession())
        {
            var u1 = new User { Name = "1" };
            await session.StoreAsync(u1, "Users/1");
            await session.SaveChangesAsync();
            u1.Name = "11";
            await session.SaveChangesAsync();

            var u2 = new User { Name = "2" };
            await session.StoreAsync(u2, "Users/2");
            await session.SaveChangesAsync();
            u2.Name = "22";
            await session.SaveChangesAsync();

            var c1 = new Company { Name = "1" };
            await session.StoreAsync(c1, "Company/1");
            await session.SaveChangesAsync();
            c1.Name = "11";
            await session.SaveChangesAsync();

            var c2 = new Company { Name = "2" };
            await session.StoreAsync(c2, "Company/2");
            await session.SaveChangesAsync();
            c2.Name = "22";
            await session.SaveChangesAsync();

            var c3 = new Company { Name = "11" };
            await session.StoreAsync(c3, "Company/11");
            await session.SaveChangesAsync();
            c3.Name = "1111";
            await session.SaveChangesAsync();
        }

        var stats = await store.Maintenance.SendAsync(new GetCollectionRevisionsStatisticsOperation());
        Assert.Equal(10, stats.CountOfRevisions);
        Assert.Equal(2, stats.Collections.Count);
        var collections = stats.Collections.Keys;
        Assert.Contains("Users", collections);
        Assert.Contains("Companies", collections);
        Assert.Equal(4, stats.Collections["Users"]);
        Assert.Equal(6, stats.Collections["Companies"]);

        var ids = await store.Maintenance.SendAsync(new GetRevisionsIdsByPrefixOperation("Use"));
        Assert.Equal(2, ids.Count);
        Assert.Contains("Users/1", ids);
        Assert.Contains("Users/2", ids);

        ids = await store.Maintenance.SendAsync(new GetRevisionsIdsByPrefixOperation("Company/1"));
        Assert.Equal(2, ids.Count);
        Assert.Contains("Company/1", ids);
        Assert.Contains("Company/11", ids);

        ids = await store.Maintenance.SendAsync(new GetRevisionsIdsByPrefixOperation("Orders/"));
        Assert.Equal(0, ids.Count);
    }

    public sealed class GetRevisionsIdsByPrefixOperation : IMaintenanceOperation<List<string>>
    {
        private string _prefix;
        
        public GetRevisionsIdsByPrefixOperation(string prefix)
        {
            _prefix = prefix;
        }

        public RavenCommand<List<string>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetRevisionsIdsByPrefixCommand(_prefix);
        }

        internal sealed class GetRevisionsIdsByPrefixCommand : RavenCommand<List<string>>
        {
            private string _prefix;

            public GetRevisionsIdsByPrefixCommand(string prefix)
            {
                _prefix = prefix;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/studio/revisions/ids?prefix={_prefix}";

                return new HttpRequestMessage { Method = HttpMethod.Get };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var results = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<AllResults>(response);
                Result = results.Results.Select(x => x.Id).ToList();
            }

            private class AllResults
            {
                public List<SingleResult> Results { get; set; }
            }

            private class SingleResult
            {
                public string Id { get; set; }
            }
        }
    }


    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Company
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Doc
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Order
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class RevisionsCollectionPreviewOperation : IMaintenanceOperation<RevisionsPreviewResults>
    {
        private readonly string _collection;
        private readonly int _start;
        private readonly int _pageSize;
        private readonly RevisionsType _type;
        private readonly string _continuationToken;

        public RevisionsCollectionPreviewOperation(string collection, RevisionsType type, int start, int pageSize)
        {
            _collection = collection;
            _start = start;
            _pageSize = pageSize;
            _type = type;
            _continuationToken = null;
        }

        public RevisionsCollectionPreviewOperation(string collection, RevisionsType type, string continuationToken)
        {
            _collection = collection;
            _continuationToken = continuationToken;
            _type = type;
        }

        public RavenCommand<RevisionsPreviewResults> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RevisionsPreviewCommand(_collection, _type, _continuationToken, _start, _pageSize);
        }

        private sealed class RevisionsPreviewCommand : RavenCommand<RevisionsPreviewResults>
        {
            private readonly string _collection;
            private readonly int _start;
            private readonly int _pageSize;
            private readonly RevisionsType _type;
            private readonly string _continuationToken;

            public RevisionsPreviewCommand(string collection, RevisionsType type, string continuationToken, int start, int pageSize)
            {
                _collection = collection;
                _start = start;
                _pageSize = pageSize;
                _type = type;
                _continuationToken = continuationToken;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/studio/revisions/preview?{Web.RequestHandler.StartParameter}={_start}&{Web.RequestHandler.PageSizeParameter}={_pageSize}&type={_type.ToString()}";

                if (string.IsNullOrEmpty(_collection) == false)
                    url += $"&collection={Uri.EscapeDataString(_collection)}";

                if (string.IsNullOrEmpty(_continuationToken) == false)
                    url += $"&continuationToken={Uri.EscapeDataString(_continuationToken)}";

                return new HttpRequestMessage { Method = HttpMethod.Get, };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    throw new InvalidOperationException();
                if (fromCache)
                {
                    // we have to clone the response here because  otherwise the cached item might be freed while
                    // we are still looking at this result, so we clone it to the side
                    response = response.Clone(context);
                }

                Result = GetRevisionsPreviewResults(response);
            }

            private static readonly Func<BlittableJsonReaderObject, RevisionsPreviewResults> GetRevisionsPreviewResults =
                JsonDeserializationBase.GenerateJsonDeserializationRoutine<RevisionsPreviewResults>();

        }

    }

    private class RevisionInfo
    {
        public string Id { get; set; }
        public int Etag { get; set; }
        public DateTime LastModified { get; set; }
        public string ChangeVector { get; set; }
        public DocumentFlags Flags { get; set; }
        public string Collection { get; set; }
        public int? ShardNumber { get; set; }
    }

    private class RevisionsPreviewResults
    {
        public int TotalResults { get; set; }
        public List<RevisionInfo> Results { get; set; }
        public string ContinuationToken { get; set; }

    }

    public enum RevisionsType
    {
        All,
        Regular,
        Deleted
    }
}

