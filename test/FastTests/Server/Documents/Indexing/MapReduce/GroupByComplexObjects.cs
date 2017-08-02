using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
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
                using (var index = MapReduceIndex.CreateNew(new IndexDefinition()
                {
                    Etag = 10,
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
                    DocumentQueryResult queryResult;
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        Put_docs(context, database);

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);

                        queryResult =
                            await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count);

                    }
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}' WHERE Location = 'Poland'"), context, OperationCancelToken.None);

                        var results = queryResult.Results;

                        Assert.Equal(1, results.Count);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal(@"{""Country"":""Poland"",""State"":""Pomerania""}", results[0].Data["Location"].ToString());
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
                using (var index = MapReduceIndex.CreateNew(new IndexDefinition()
                {
                    Etag = 10,
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
                    DocumentQueryResult queryResult;
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        Put_docs(context, database);

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);

                        queryResult =
                            await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count);

                    }
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}' WHERE Hobbies = 'music'"), context, OperationCancelToken.None);

                        var results = queryResult.Results;

                        Assert.Equal(1, results.Count);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("music", ((BlittableJsonReaderArray)results[0].Data["Hobbies"])[0].ToString());
                        Assert.Equal("sport", ((BlittableJsonReaderArray)results[0].Data["Hobbies"])[1].ToString());
                        Assert.Equal(2L, results[0].Data["Count"]);

                        foreach (var document in results)
                        {
                            document.Data.Dispose();
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task By_multiple_complex_objects()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapReduceIndex.CreateNew(new IndexDefinition()
                {
                    Etag = 10,
                    Name = "Users_GroupByLocationAndResidenceAddress",
                    Maps = { @"from user in docs.Users select new { 
                                user.Location,
                                user.ResidenceAddress,
                                Count = 1
                            }" },
                    Reduce = @"from result in results group result by new { result.Location, result.ResidenceAddress } into g select new { 
                                g.Key.Location, 
                                g.Key.ResidenceAddress,
                                Count = g.Sum(x => x.Count)
                            }",
                    Fields = new Dictionary<string, IndexFieldOptions>()
                    {
                        { "Location", new IndexFieldOptions()
                            {
                                Indexing = FieldIndexing.Analyzed,
                            }
                        },
                        { "ResidenceAddress", new IndexFieldOptions()
                            {
                                Indexing = FieldIndexing.Analyzed,
                            }
                        }
                    }
                }, database))
                {
                    DocumentQueryResult queryResult;
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        Put_docs(context, database);

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        while (index.DoIndexingWork(scope, CancellationToken.None))
                        {

                        }

                        queryResult =
                            await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count);

                    }

                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {

                        queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}' WHERE Location = 'Poland'"), context, OperationCancelToken.None);

                        var results = queryResult.Results;

                        Assert.Equal(1, results.Count);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal(@"{""Country"":""Poland"",""State"":""Pomerania""}", results[0].Data["Location"].ToString());
                        Assert.Equal(@"{""Country"":""UK""}", results[0].Data["ResidenceAddress"].ToString());
                        Assert.Equal(2L, results[0].Data["Count"]);
                    }
                }
            }
        }

        [Fact]
        public async Task By_complex_object_and_array()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapReduceIndex.CreateNew(new IndexDefinition()
                {
                    Etag = 10,
                    Name = "Users_GroupByLocationAndResidenceAddress",
                    Maps = { @"from user in docs.Users select new { 
                                user.Hobbies,
                                user.ResidenceAddress,
                                Count = 1
                            }" },
                    Reduce = @"from result in results group result by new { result.Hobbies, result.ResidenceAddress } into g select new { 
                                g.Key.Hobbies, 
                                g.Key.ResidenceAddress,
                                Count = g.Sum(x => x.Count)
                            }",
                    Fields = new Dictionary<string, IndexFieldOptions>()
                    {
                        { "Hobbies", new IndexFieldOptions()
                            {
                                Indexing = FieldIndexing.Analyzed,
                            }
                        },
                        { "ResidenceAddress", new IndexFieldOptions()
                            {
                                Indexing = FieldIndexing.Analyzed,
                            }
                        }
                    }
                }, database))
                {
                    DocumentQueryResult queryResult;
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        Put_docs(context, database);

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        while (index.DoIndexingWork(scope, CancellationToken.None))
                        {

                        }

                        queryResult =
                            await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count);

                    }
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {

                        queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}' WHERE Hobbies IN ('music')"), context, OperationCancelToken.None);

                        var results = queryResult.Results;

                        Assert.Equal(1, results.Count);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("music", ((BlittableJsonReaderArray)results[0].Data["Hobbies"])[0].ToString());
                        Assert.Equal("sport", ((BlittableJsonReaderArray)results[0].Data["Hobbies"])[1].ToString());
                        Assert.Equal(@"{""Country"":""UK""}", results[0].Data["ResidenceAddress"].ToString());
                        Assert.Equal(2L, results[0].Data["Count"]);
                    }
                }
            }
        }

        private static void Put_docs(DocumentsOperationContext context, DocumentDatabase database)
        {
            using (var tx = context.OpenWriteTransaction())
            {
                using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                {
                    ["Location"] = new DynamicJsonValue
                    {
                        ["Country"] = "USA",
                        ["State"] = "Texas"
                    },
                    ["ResidenceAddress"] = new DynamicJsonValue
                    {
                        ["Country"] = "UK"
                    },
                    ["Hobbies"] = new DynamicJsonArray
                    {
                        "sport", "books"
                    },
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = "Users"
                    }
                }))
                {
                    database.DocumentsStorage.Put(context, "users/1", null, doc);
                }

                using (var doc = CreateDocument(context, "users/2", new DynamicJsonValue
                {
                    ["Location"] = new DynamicJsonValue()
                    {
                        ["Country"] = "Poland",
                        ["State"] = "Pomerania"
                    },
                    ["ResidenceAddress"] = new DynamicJsonValue
                    {
                        ["Country"] = "UK"
                    },
                    ["Hobbies"] = new DynamicJsonArray()
                    {
                        "music", "sport"
                    },
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = "Users"
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
                    ["Location"] = new DynamicJsonValue()
                    {
                        ["State"] = "Pomerania",
                        ["Country"] = "Poland"
                    },
                    ["ResidenceAddress"] = new DynamicJsonValue
                    {
                        ["Country"] = "UK"
                    },
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Collection] = "Users"
                    }
                }))
                {
                    database.DocumentsStorage.Put(context, "users/3", null, doc);
                }

                tx.Commit();
            }
        }
    }
}