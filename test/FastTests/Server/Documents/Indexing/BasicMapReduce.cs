using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicMapReduce : RavenTestBase
    {
        //TODO: Create base class for indexing tests with methods like
        //TODO: LowLevel_CreateDocumentDatabase
        [Fact]
        public async Task CanUseSimpleReduction()
        {
            using (var db = LowLevel_CreateDocumentDatabase())
            {
                var mri = AutoMapReduceIndex.CreateNew(1,
                    new AutoMapReduceIndexDefinition("test", new[] {"Users"}, new[]
                    {
                        new IndexField
                        {
                            Name = "Count",
                            MapReduceOperation = FieldMapReduceOperation.Count,
                            Storage = FieldStorage.Yes
                        }
                    }, new[]
                    {
                        new IndexField
                        {
                            Name = "Location",
                            Storage = FieldStorage.Yes
                        },
                    }), db);

                using (mri)
                {
                    CreateUsers(db, 2, "Poland");

                    var stats = new IndexingBatchStats();
                    mri.DoIndexingWork(stats, CancellationToken.None);

                    using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                    {
                        var queryResult = await mri.Query(new  IndexQuery(), context, CancellationToken.None);
                        
                        Assert.Equal(1, queryResult.Results.Count);
                        var result = queryResult.Results[0].Data;

                        string location;
                        Assert.True(result.TryGet("Location", out location));
                        Assert.Equal("Poland", location);

                        var count = result["Count"] as LazyDoubleValue;
                        
                        Assert.NotNull(count);
                        Assert.Equal("2.0", count.Inner.ToString());
                    }

                    using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                    {
                        var queryResult = await mri.Query(new IndexQuery()
                        {
                            Query = "Count_Range:[Lx2 TO Lx10]"
                        }, context, CancellationToken.None);

                        Assert.Equal(1, queryResult.Results.Count);

                        queryResult = await mri.Query(new IndexQuery()
                        {
                            Query = "Count_Range:[Lx10 TO NULL]"
                        }, context, CancellationToken.None);

                        Assert.Equal(0, queryResult.Results.Count);
                    }
                }
            }
        }

        [Fact]
        public async Task MultipleReduceKeys()
        {
            using (var db = LowLevel_CreateDocumentDatabase())
            {
                var mri = AutoMapReduceIndex.CreateNew(1,
                    new AutoMapReduceIndexDefinition("test", new[] { "Users" }, new[]
                    {
                        new IndexField
                        {
                            Name = "Count",
                            MapReduceOperation = FieldMapReduceOperation.Count,
                            Storage = FieldStorage.Yes
                        }
                    }, new[]
                    {
                        new IndexField
                        {
                            Name = "Location",
                            Storage = FieldStorage.Yes
                        },
                    }), db);

                using (mri)
                {
                    CreateUsers(db, 100, "Poland", "Israel", "USA");

                    var stats = new IndexingBatchStats();
                    mri.DoIndexingWork(stats, CancellationToken.None);

                    using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                    {
                        var queryResult = await mri.Query(new IndexQuery(), context, CancellationToken.None);

                        var results = queryResult.Results;

                        Assert.Equal(3, results.Count);

                        Assert.Equal("Poland", results[0].Data["Location"].ToString());
                        Assert.Equal("34.0", ((LazyDoubleValue) results[0].Data["Count"]).Inner.ToString());

                        Assert.Equal("Israel", results[1].Data["Location"].ToString());
                        Assert.Equal("33.0", ((LazyDoubleValue)results[1].Data["Count"]).Inner.ToString());

                        Assert.Equal("USA", results[2].Data["Location"].ToString());
                        Assert.Equal("33.0", ((LazyDoubleValue)results[2].Data["Count"]).Inner.ToString());
                    }
                }
            }
        }

        private static void CreateUsers(DocumentDatabase db, int numberOfUsers, params string[] locations)
        {
            using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    for (int i = 0; i < numberOfUsers; i++)
                    {
                        using (var doc = context.ReadObject(new DynamicJsonValue
                        {
                            ["Name"] = $"User-{i}",
                            ["Location"] = locations[i % locations.Length],
                            [Constants.Metadata] = new DynamicJsonValue
                            {
                                [Constants.RavenEntityName] = "Users"
                            }
                        }, $"users/{i}"))
                        {
                            db.DocumentsStorage.Put(context, $"users/{i}", null, doc);
                        }
                    }

                    tx.Commit();
                }
            }
        }
    }
}