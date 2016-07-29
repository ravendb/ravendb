using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing.MapReduce
{
    public class GroupByComplexObjects : RavenLowLevelTestBase
    {
        [Fact]
        public async Task By_single_complex_object()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapReduceIndex.CreateNew(1, new IndexDefinition()
                {
                    Name = "Users_ByCount_GroupByLocation",
                    Maps = { @"from user in docs.Users select new { 
                                user.Location,
                                Count = 1
                            }" },
                    Reduce = @"from result in results group result by result.Location into g select new { 
                                Location = g.Key, 
                                Count = g.Sum(x => x.Count)
                            }",
                    Fields = new Dictionary<string, IndexFieldOptions>()
                    {
                        { "Location", new IndexFieldOptions()
                        {
                            Indexing = FieldIndexing.Analyzed,
                        } }
                    }
                }, database))
                {
                    using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                            {
                                ["Location"] = new DynamicJsonValue()
                                {
                                    ["Country"] = "Poland"
                                },
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
                                ["Location"] = new DynamicJsonValue()
                                {
                                    ["Country"] = "Poland"
                                },
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

                        var queryResult = await index.Query(new IndexQueryServerSide(), context, OperationCancelToken.None);

                        Assert.Equal(1, queryResult.Results.Count);

                        context.Reset();

                        queryResult = await index.Query(new IndexQueryServerSide() { Query = @"Location:Poland" }, context, OperationCancelToken.None);

                        var results = queryResult.Results;

                        Assert.Equal(1, results.Count);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal(@"{""Country"":""Poland""}", results[0].Data["Location"].ToString());
                        Assert.Equal(2L, results[0].Data["Count"]);
                    }
                }
            }
        }

        [Fact]
        public async Task By_single_array_object()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapReduceIndex.CreateNew(1, new IndexDefinition()
                {
                    Name = "Users_GroupByHobbies",
                    Maps = { @"from user in docs.Users select new { 
                                user.Hobbies,
                                Count = 1
                            }" },
                    Reduce = @"from result in results group result by result.Hobbies into g select new { 
                                Hobbies = g.Key, 
                                Count = g.Sum(x => x.Count)
                            }",
                    Fields = new Dictionary<string, IndexFieldOptions>()
                    {
                        { "Hobbies", new IndexFieldOptions()
                            {
                                Indexing = FieldIndexing.Analyzed,
                            }
                        }
                    }
                }, database))
                {
                    using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                            {
                                ["Hobbies"] = new DynamicJsonArray()
                                {
                                    "sport", "books"
                                },
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
                                ["Hobbies"] = new DynamicJsonArray()
                                {
                                    "music", "sport"
                                },
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.Headers.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "users/2", null, doc);
                            }

                            using (var doc = CreateDocument(context, "users/3", new DynamicJsonValue
                            {
                                ["Hobbies"] = new DynamicJsonArray()
                                {
                                    "music", "sport"
                                },
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.Headers.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "users/3", null, doc);
                            }

                            tx.Commit();
                        }

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);

                        var queryResult = await index.Query(new IndexQueryServerSide(), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count);

                        context.Reset();

                        queryResult = await index.Query(new IndexQueryServerSide() { Query = @"Hobbies:music" }, context, OperationCancelToken.None);

                        var results = queryResult.Results;

                        Assert.Equal(1, results.Count);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("music", ((BlittableJsonReaderArray)results[0].Data["Hobbies"])[0].ToString());
                        Assert.Equal("sport", ((BlittableJsonReaderArray)results[0].Data["Hobbies"])[1].ToString());
                        Assert.Equal(2L, results[0].Data["Count"]);
                    }
                }
            }
        }
    }
}