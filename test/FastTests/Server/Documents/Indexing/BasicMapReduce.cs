using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicMapReduce : RavenLowLevelTestBase
    {
        //TODO: Create base class for indexing tests with methods like
        //TODO: CreateDocumentDatabase
        [Fact]
        public async Task CanUseSimpleReduction()
        {
            using (var db = CreateDocumentDatabase())
            using (var mri = AutoMapReduceIndex.CreateNew(1, GetUsersCountByLocationIndexDefinition(), db))
            {
                CreateUsers(db, 2, "Poland");
                
                mri.DoIndexingWork(new IndexingBatchStats(), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await mri.Query(new IndexQuery(), context, CancellationToken.None);

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

        [Fact]
        public async Task MultipleReduceKeys()
        {
            using (var db = CreateDocumentDatabase())
            using (var mri = AutoMapReduceIndex.CreateNew(1, GetUsersCountByLocationIndexDefinition(), db))
            {
                CreateUsers(db, 100, "Poland", "Israel", "USA");
                
                mri.DoIndexingWork(new IndexingBatchStats(), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await mri.Query(new IndexQuery()
                    {
                        WaitForNonStaleResultsTimeout = TimeSpan.FromMinutes(1)
                    }, context, CancellationToken.None);

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

        [Fact]
        public async Task CanDelete()
        {
            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(1, GetUsersCountByLocationIndexDefinition(), db))
            {
                CreateUsers(db, 10, "Poland");
                
                index.DoIndexingWork(new IndexingBatchStats(), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await index.Query(new IndexQuery(), context, CancellationToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal("10.0", ((LazyDoubleValue)results[0].Data["Count"]).Inner.ToString());
                }

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        db.DocumentsStorage.Delete(context, "users/1", null);

                        tx.Commit();
                    }
                }

                index.DoIndexingWork(new IndexingBatchStats(), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await index.Query(new IndexQuery(), context, CancellationToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal("9.0", ((LazyDoubleValue)results[0].Data["Count"]).Inner.ToString());
                }
            }
        }

        [Fact(Skip = "TODO arek")]
        public void IndexLoadsEtagOfLastMapResultOnInitialize()
        {
            var path = NewDataPath();
            using (var db = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var id = db.IndexStore.CreateIndex(GetUsersCountByLocationIndexDefinition());


                var index = (AutoMapReduceIndex) db.IndexStore.GetIndex(id);

                Assert.Equal(-1, index._lastMapResultEtag);

                CreateUsers(db, 10, "Poland");
                index.DoIndexingWork(new IndexingBatchStats(), CancellationToken.None);

                Assert.Equal(9, index._lastMapResultEtag);
            }

            using (var db = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                Index index = null;
                Assert.True(SpinWait.SpinUntil(() => (index = db.IndexStore.GetIndex(1)) != null, TimeSpan.FromSeconds(15)));

                Assert.Equal(9, ((AutoMapReduceIndex) index)._lastMapResultEtag);
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

        private static AutoMapReduceIndexDefinition GetUsersCountByLocationIndexDefinition()
        {
            return new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
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
            });
        }
    }
}