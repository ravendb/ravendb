using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class BasicStaticMapIndexing : RavenLowLevelTestBase
    {
        [Fact]
        public async Task The_easiest_static_index()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = StaticMapIndex.CreateNew(1, new IndexDefinition()
                {
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map
                }, database))
                {
                    using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                            {
                                ["Name"] = "John",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.Headers.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "users/1", null, doc);
                            }

                            using (var doc = CreateDocument(context, "users/2", new DynamicJsonValue
                            {
                                ["Name"] = "Edward",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.Headers.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "users/2", null, doc);
                            }

                            tx.Commit();
                        }

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);

                        Assert.Equal(2, batchStats.MapAttempts);
                        Assert.Equal(2, batchStats.MapSuccesses);
                        Assert.Equal(0, batchStats.MapErrors);

                        var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count);

                        context.Reset();

                        queryResult = await index.Query(new IndexQuery() { Query = "Name:John" }, context, OperationCancelToken.None);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("users/1", queryResult.Results[0].Key);
                    }
                }
            }
        }

        [Fact(Skip = "TODO arek - need to filter out duplicates")]
        public async Task Static_index_with_multiple_indexing_fuctions_for_single_collection()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = StaticMapIndex.CreateNew(1, new IndexDefinition
                {
                    Name = "Users_ByName_multi_functions", // TODO arek - name changed because of the same assembly name as in the above test
                    Maps =
                    {
                        "from user in docs.Users select new { Name = user.Name }",
                        "from user in docs.Users select new { Name = user.Name2 }"
                    },
                    Type = IndexType.Map
                }, database))
                {
                    using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                            {
                                ["Name"] = "David",
                                ["Name2"] = "John",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.Headers.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "users/1", null, doc);
                            }

                            using (var doc = CreateDocument(context, "users/2", new DynamicJsonValue
                            {
                                ["Name"] = "Edward",
                                ["Name2"] = "Matthew",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.Headers.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "users/2", null, doc);
                            }

                            tx.Commit();
                        }

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);

                        Assert.Equal(4, batchStats.MapAttempts);
                        Assert.Equal(4, batchStats.MapSuccesses);
                        Assert.Equal(0, batchStats.MapErrors);

                        var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count); // TODO arek - duplicates

                        context.Reset();

                        queryResult = await index.Query(new IndexQuery() { Query = "Name:John" }, context, OperationCancelToken.None);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("users/1", queryResult.Results[0].Key);

                        context.Reset();

                        queryResult = await index.Query(new IndexQuery() { Query = "Name:David" }, context, OperationCancelToken.None);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("users/1", queryResult.Results[0].Key);
                    }
                }
            }
        }
    }
}