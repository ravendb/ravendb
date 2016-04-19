using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicMapReduce : RavenLowLevelTestBase
    {
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
                    var queryResult = await mri.Query(new IndexQuery(), context, OperationCancelToken.None);

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
                    }, context, OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);

                    queryResult = await mri.Query(new IndexQuery()
                    {
                        Query = "Count_Range:[Lx10 TO NULL]"
                    }, context, OperationCancelToken.None);

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
                    }, context, OperationCancelToken.None);

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
                    var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

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
                    var queryResult = await index.Query(new IndexQuery(), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal("9.0", ((LazyDoubleValue)results[0].Data["Count"]).Inner.ToString());
                }
            }
        }

        [Fact]
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
                var index = db.IndexStore.GetIndex(1);

                Assert.Equal(9, ((AutoMapReduceIndex) index)._lastMapResultEtag);
            }
        }

        [Fact]
        public void DefinitionOfAutoMapReduceIndexIsPersisted()
        {
            var path = NewDataPath();
            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var count = new IndexField
                {
                    Name = "Count",
                    Highlighted = true,
                    Storage = FieldStorage.Yes,
                    SortOption = SortOptions.NumericDefault,
                    MapReduceOperation = FieldMapReduceOperation.Count
                };

                var location = new IndexField
                {
                    Name = "Location",
                    Highlighted = true,
                    Storage = FieldStorage.Yes,
                    SortOption = SortOptions.String,
                };

                Assert.Equal(1, database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition(new [] { "Users" }, new[] { count }, new[] { location })));

                var sum = new IndexField
                {
                    Name = "Sum",
                    Highlighted = false,
                    Storage = FieldStorage.Yes,
                    SortOption = SortOptions.NumericDefault,
                    MapReduceOperation = FieldMapReduceOperation.Sum
                };

                Assert.Equal(2, database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition(new[] { "Users" }, new[] { count, sum }, new[] { location })));

                var index2 = database.IndexStore.GetIndex(2);
                index2.SetLock(IndexLockMode.LockedError);
                index2.SetPriority(IndexingPriority.Disabled);
            }

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var indexes = database
                    .IndexStore
                    .GetIndexesForCollection("Users")
                    .OrderBy(x => x.IndexId)
                    .ToList();

                Assert.Equal(2, indexes.Count);

                Assert.Equal(1, indexes[0].IndexId);
                Assert.Equal(1, indexes[0].Definition.Collections.Length);
                Assert.Equal("Users", indexes[0].Definition.Collections[0]);
                Assert.Equal(1, indexes[0].Definition.MapFields.Count);
                Assert.Equal("Count", indexes[0].Definition.MapFields["Count"].Name);
                Assert.Equal(SortOptions.NumericDefault, indexes[0].Definition.MapFields["Count"].SortOption);
                Assert.True(indexes[0].Definition.MapFields["Count"].Highlighted);
                Assert.Equal(FieldMapReduceOperation.Count, indexes[0].Definition.MapFields["Count"].MapReduceOperation);

                var definition = indexes[0].Definition as AutoMapReduceIndexDefinition;

                Assert.NotNull(definition);

                Assert.Equal(1, definition.GroupByFields.Length);
                Assert.Equal("Location", definition.GroupByFields[0].Name);
                Assert.Equal(SortOptions.String, definition.GroupByFields[0].SortOption);

                Assert.Equal(IndexLockMode.Unlock, indexes[0].Definition.LockMode);
                Assert.Equal(IndexingPriority.Normal, indexes[0].Priority);
                
                Assert.Equal(2, indexes[1].IndexId);
                Assert.Equal(1, indexes[1].Definition.Collections.Length);
                Assert.Equal("Users", indexes[1].Definition.Collections[0]);

                Assert.Equal(2, indexes[1].Definition.MapFields.Count);
                Assert.Equal("Count", indexes[1].Definition.MapFields["Count"].Name);
                Assert.Equal(FieldMapReduceOperation.Count, indexes[1].Definition.MapFields["Count"].MapReduceOperation);
                Assert.Equal(SortOptions.NumericDefault, indexes[1].Definition.MapFields["Count"].SortOption);
                Assert.Equal("Sum", indexes[1].Definition.MapFields["Sum"].Name);
                Assert.Equal(FieldMapReduceOperation.Sum, indexes[1].Definition.MapFields["Sum"].MapReduceOperation);
                Assert.Equal(SortOptions.NumericDefault, indexes[1].Definition.MapFields["Sum"].SortOption);

                definition = indexes[0].Definition as AutoMapReduceIndexDefinition;

                Assert.NotNull(definition);

                Assert.Equal(1, definition.GroupByFields.Length);
                Assert.Equal("Location", definition.GroupByFields[0].Name);
                Assert.Equal(SortOptions.String, definition.GroupByFields[0].SortOption);

                Assert.Equal(IndexLockMode.LockedError, indexes[1].Definition.LockMode);
                Assert.Equal(IndexingPriority.Disabled, indexes[1].Priority);
            }
        }

        [Fact]
        public async Task MultipleAggregationFunctionsCanBeUsed()
        {
            using (var db = CreateDocumentDatabase())
            using (var mri = AutoMapReduceIndex.CreateNew(1, new AutoMapReduceIndexDefinition(new[] { "Users" }, new[]
            {
                new IndexField
                {
                    Name = "Count",
                    MapReduceOperation = FieldMapReduceOperation.Count,
                    Storage = FieldStorage.Yes
                },
                new IndexField
                {
                    Name = "TotalCount",
                    MapReduceOperation = FieldMapReduceOperation.Count,
                    Storage = FieldStorage.Yes
                },
                new IndexField
                {
                    Name = "Age",
                    MapReduceOperation = FieldMapReduceOperation.Sum,
                    Storage = FieldStorage.Yes
                }
            }, new[]
            {
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes
                },
            }), db))
            {
                CreateUsers(db, 2, "Poland");

                mri.DoIndexingWork(new IndexingBatchStats(), CancellationToken.None);

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await mri.Query(new IndexQuery(), context, OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);
                    var result = queryResult.Results[0].Data;

                    string location;
                    Assert.True(result.TryGet("Location", out location));
                    Assert.Equal("Poland", location);

                    var count = result["Count"] as LazyDoubleValue;

                    Assert.NotNull(count);
                    Assert.Equal("2.0", count.Inner.ToString());

                    var totalCount = result["TotalCount"] as LazyDoubleValue;

                    Assert.NotNull(totalCount);
                    Assert.Equal("2.0", totalCount.Inner.ToString());

                    var age = result["Age"] as LazyDoubleValue;

                    Assert.NotNull(age);
                    Assert.Equal("41.0", age.Inner.ToString());
                }

                using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), db))
                {
                    var queryResult = await mri.Query(new IndexQuery()
                    {
                        Query = "Count_Range:[Lx2 TO Lx10]"
                    }, context, OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);

                    queryResult = await mri.Query(new IndexQuery()
                    {
                        Query = "Count_Range:[Lx10 TO NULL]"
                    }, context, OperationCancelToken.None);

                    Assert.Equal(0, queryResult.Results.Count);
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
                            ["Age"] = 20 + i,
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