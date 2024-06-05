using System;
using System.Collections.Generic;
using System.Drawing.Printing;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Amazon.Runtime.Documents;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.Schemas;
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

    [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Voron | RavenTestCategory.Sharding)]
    public async Task TestAllRevisionsViewForSharding()
    {
        using var store = Sharding.GetDocumentStore();

        var list = await Initialize(store);

        Func<RevisionInfo, Task<string>> getName = async (result) =>
        {
            var shardNumber = await Sharding.GetShardNumberForAsync(store, result.Id);

            var db = await Sharding.GetAnyShardDocumentDatabaseInstanceFor($"{store.Database}${shardNumber}");

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var revision = db.DocumentsStorage.RevisionsStorage.GetRevision(ctx, result.ChangeVector);
                if (revision.Data.TryGet("Name", out string name) == false)
                    throw new InvalidOperationException($"revision data ({result.ChangeVector}, {result.Id}) doesn't contains field 'Name'.");

                return name;

            }

        };

        await AssertResults(store, getName, list, 8, 0, 3);
        await AssertResults(store, getName, list, 8, 1, 3);
        await AssertResults(store, getName, list, 8, 2, 3);
        await AssertResults(store, getName, list, 8, 0, 8);

    }



    [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Voron)]
    public async Task TestAllRevisionsView()
    {
        using var store = GetDocumentStore();

        var list = await Initialize(store);

        var db = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
        using (ctx.OpenReadTransaction())
        {
            var usersLastCv = db.DocumentsStorage.RevisionsStorage.GetLastRevisionChangeVectorForCollection(ctx, "Users");
            var docsLastCv = db.DocumentsStorage.RevisionsStorage.GetLastRevisionChangeVectorForCollection(ctx, "Docs");
            var lastCv = db.DocumentsStorage.RevisionsStorage.GetLastRevisionChangeVector(ctx);

            Assert.Equal(lastCv, docsLastCv);

            var lastEtag = Convert.ToInt64(lastCv.Split(":")[1].Split("-")[0]);
            var lastUsersEtag = Convert.ToInt64(usersLastCv.Split(":")[1].Split("-")[0]);

            Assert.True(lastEtag > lastUsersEtag, $"lastCv {lastCv}, lastEtag {lastEtag}, usersLastCv: {usersLastCv}, lastUsersEtag {lastUsersEtag}");

            Func<RevisionInfo, Task<string>> getName = (result) =>
            {
                var revision = db.DocumentsStorage.RevisionsStorage.GetRevision(ctx, result.ChangeVector);
                if (revision.Data.TryGet("Name", out string name) == false)
                    throw new InvalidOperationException($"revision data ({result.ChangeVector}, {result.Id}) doesn't contains field 'Name'.");

                return Task.FromResult(name);
            };

            await AssertResults(store, getName, list, 8, 0, 3);
            await AssertResults(store, getName, list, 8, 1, 3);
            await AssertResults(store, getName, list, 8, 2, 3);
            await AssertResults(store, getName, list, 8, 0, 8);
        }
    }


    private async Task<List<(string Id, string Name)>> Initialize(DocumentStore store)
    {
        var configuration = new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false
            }
        };
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
            await Task.Delay(100);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = i.ToString() }, "Users/2");
                list.Add(("Users/2", i.ToString()));
                await session.SaveChangesAsync();
            }
            await Task.Delay(100);
        }

        for (int i = 0; i < 2; i++)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Doc { Name = i.ToString() }, "Docs/1");
                list.Add(("Docs/1", i.ToString()));
                await session.SaveChangesAsync();
            }
            await Task.Delay(100);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Doc { Name = i.ToString() }, "Docs/2");
                list.Add(("Docs/2", i.ToString()));
                await session.SaveChangesAsync();
            }
            await Task.Delay(100);

        }

        list.Reverse();

        return list;
    }

    private async Task AssertResults(DocumentStore store, Func<RevisionInfo, Task<string>> getName, List<(string Id, string Name)> list,
        int totalResults, int start, int pageSize, string collection = null)
    {
        var results = await store.Maintenance.SendAsync(new RevisionsCollectionPreviewOperation(collection, start, pageSize));
        Assert.Equal(totalResults, results.TotalResults);
        Assert.Equal(pageSize, results.Results.Count);

        int i = start;
        foreach (var result in results.Results)
        {
            var info = list[i++];

            Assert.Equal(info.Id, result.Id);

            var name = await getName(result);

            Assert.Equal(info.Name, name);
        }
    }


    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Doc
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class RevisionsCollectionPreviewOperation : IMaintenanceOperation<RevisionsPreviewResults>
    {
        private readonly string _collection;
        private readonly int _start;
        private readonly int _pageSize;

        public RevisionsCollectionPreviewOperation(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public RevisionsCollectionPreviewOperation(string collection, int start, int pageSize)
        {
            _collection = collection;
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<RevisionsPreviewResults> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RevisionsPreviewCommand(_collection, _start, _pageSize);
        }

        private sealed class RevisionsPreviewCommand : RavenCommand<RevisionsPreviewResults>
        {
            private readonly string _collection;
            private readonly int _start;
            private readonly int _pageSize;


            public RevisionsPreviewCommand(string collection, int start, int pageSize)
            {
                _collection = collection;
                _start = start;
                _pageSize = pageSize;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/studio/revisions/preview?{Web.RequestHandler.StartParameter}={_start}&{Web.RequestHandler.PageSizeParameter}={_pageSize}";

                if (string.IsNullOrEmpty(_collection) == false)
                    url += $"&collection={Uri.EscapeDataString(_collection)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };
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

            private static readonly Func<BlittableJsonReaderObject, RevisionsPreviewResults> GetRevisionsPreviewResults = JsonDeserializationBase.GenerateJsonDeserializationRoutine<RevisionsPreviewResults>();

        }

    }

    private class RevisionInfo
    {
        public string Id { get; set; }
        public int Etag { get; set; }
        public DateTime LastModified { get; set; }
        public string ChangeVector { get; set; }
        public DocumentFlags Flags { get; set; }
        public int? ShardNumber { get; set; }
    }

    private class RevisionsPreviewResults
    {
        public int TotalResults { get; set; }
        public List<RevisionInfo> Results { get; set; }
        public string ContinuationToken { get; set; }

    }
}

