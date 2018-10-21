using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Auto
{
    [SuppressMessage("ReSharper", "ConsiderUsingConfigureAwait")]
    public class BasicAutoMapReduceIndexing : RavenLowLevelTestBase
    {
        [Fact]
        public async Task CanUseSimpleReduction()
        {
            using (var db = CreateDocumentDatabase())
            using (var mri = AutoMapReduceIndex.CreateNew(GetUsersCountByLocationIndexDefinition(), db))
            {
                CreateUsers(db, 2, "Poland");

                mri.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await mri.Query(new IndexQueryServerSide($"FROM INDEX '{mri.Name}'"), context, OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);
                    var result = queryResult.Results[0].Data;

                    string location;
                    Assert.True(result.TryGet("Location", out location));
                    Assert.Equal("Poland", location);

                    var count = result["Count"];

                    Assert.Equal(2L, count);
                }

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await mri.Query(new IndexQueryServerSide($"FROM INDEX '{mri.Name}' WHERE Count BETWEEN 2 AND 10"), context,
                        OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);
                }
                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await mri.Query(new IndexQueryServerSide($"FROM INDEX '{mri.Name}' WHERE Count >= 10"), context, OperationCancelToken.None);

                    Assert.Equal(0, queryResult.Results.Count);
                }
            }
        }

        [Theory]
        [InlineData(100, new[] { "Poland", "Israel", "USA" })]
        public async Task MultipleReduceKeys(int numberOfUsers, string[] locations)
        {
            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(GetUsersCountByLocationIndexDefinition(), db))
            {
                CreateUsers(db, numberOfUsers, locations);

                var batchStats = new IndexingRunStats();
                var scope = new IndexingStatsScope(batchStats);

                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
                while (index.DoIndexingWork(scope, cts.Token));

                Assert.Equal(numberOfUsers, batchStats.MapAttempts);
                Assert.Equal(numberOfUsers, batchStats.MapSuccesses);
                Assert.Equal(0, batchStats.MapErrors);
                Assert.True(batchStats.ReduceAttempts >= numberOfUsers, $"{batchStats.ReduceAttempts} >= {numberOfUsers}");
                Assert.True(batchStats.ReduceSuccesses >= numberOfUsers, $"{batchStats.ReduceSuccesses} >= {numberOfUsers}");
                Assert.Equal(batchStats.ReduceAttempts, batchStats.ReduceSuccesses);
                Assert.Equal(0, batchStats.ReduceErrors);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'")
                    {
                        WaitForNonStaleResultsTimeout = TimeSpan.FromMinutes(1)
                    }, context, OperationCancelToken.None);

                    Assert.False(queryResult.IsStale);

                    var results = queryResult.Results;

                    Assert.Equal(locations.Length, results.Count);

                    for (int i = 0; i < locations.Length; i++)
                    {
                        Assert.Equal(locations[i], results[i].Data["Location"].ToString());

                        long expected = numberOfUsers / locations.Length + numberOfUsers % (locations.Length - i);
                        Assert.Equal(expected, results[i].Data["Count"]);
                    }
                }
            }
        }

        [Fact]
        public async Task CanDelete()
        {
            const long numberOfUsers = 10;

            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(GetUsersCountByLocationIndexDefinition(), db))
            {

                CreateUsers(db, numberOfUsers, "Poland");

                // index 10 users
                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(numberOfUsers, results[0].Data["Count"]);
                }

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        db.DocumentsStorage.Delete(context, "users/0", null);

                        tx.Commit();
                    }
                }

                // one document deleted
                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(numberOfUsers - 1, results[0].Data["Count"]);
                }

                CreateUsers(db, 1, "Poland");

                // document added again
                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(numberOfUsers, results[0].Data["Count"]);
                }

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        for (int i = 0; i < numberOfUsers; i++)
                        {
                            db.DocumentsStorage.Delete(context, $"users/{i}", null);
                        }

                        tx.Commit();
                    }
                }

                // all documents removed
                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(0, results.Count);
                }

                CreateUsers(db, numberOfUsers, "Poland");

                // documents added back
                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(numberOfUsers, results[0].Data["Count"]);
                }
            }
        }

        [Fact]
        public async Task DefinitionOfAutoMapReduceIndexIsPersisted()
        {
            string dbName;

            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
            {
                dbName = database.Name;

                var count = new AutoIndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    Aggregation = AggregationOperation.Count
                };

                var location = new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                    Indexing = AutoFieldIndexing.Search | AutoFieldIndexing.Exact | AutoFieldIndexing.Default,
                    GroupByArrayBehavior = GroupByArrayBehavior.ByContent
                };

                Assert.NotNull(await database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition("Users", new[] { count }, new[] { location })));

                var sum = new AutoIndexField
                {
                    Name = "Sum",
                    Storage = FieldStorage.Yes,
                    Aggregation = AggregationOperation.Sum
                };

                var index = await database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition("Users", new[] { count, sum }, new[] { location }));
                Assert.NotNull(index);

                var task1 = database.IndexStore.SetLock(index.Name, IndexLockMode.LockedError);
                var task2 = database.IndexStore.SetPriority(index.Name, IndexPriority.High);
                index.SetState(IndexState.Disabled);
                var task = Task.WhenAll(task1, task2);
                
                await Assert.ThrowsAsync<NotSupportedException>(async () => await task);
                Assert.StartsWith("'Lock Mode' can't be set for the Auto-Index", task1.Exception.InnerException.Message);

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(dbName);

                database = await GetDatabase(dbName);

                var indexes = database
                    .IndexStore
                    .GetIndexesForCollection("Users")
                    .OrderBy(x=>x.Name.Length) // smaller index has lesser fields
                    .ToList();

                Assert.Equal(2, indexes.Count);

                Assert.Equal(1, indexes[0].Definition.Collections.Count);
                Assert.Equal("Users", indexes[0].Definition.Collections.Single());
                Assert.Equal(1, indexes[0].Definition.MapFields.Count);
                Assert.Equal("Count", indexes[0].Definition.MapFields["Count"].Name);
                Assert.Equal(AggregationOperation.Count, indexes[0].Definition.MapFields["Count"].As<AutoIndexField>().Aggregation);

                var definition = indexes[0].Definition as AutoMapReduceIndexDefinition;

                Assert.NotNull(definition);

                Assert.Equal(1, definition.GroupByFields.Count);
                Assert.Equal("Location", definition.GroupByFields["Location"].Name);
                Assert.Equal(AutoFieldIndexing.Search | AutoFieldIndexing.Exact | AutoFieldIndexing.Default, definition.GroupByFields["Location"].Indexing);
                Assert.Equal(GroupByArrayBehavior.ByContent, definition.GroupByFields["Location"].GroupByArrayBehavior);

                Assert.Equal(IndexLockMode.Unlock, indexes[0].Definition.LockMode);
                Assert.Equal(IndexPriority.Normal, indexes[0].Definition.Priority);
                Assert.Equal(IndexState.Normal, indexes[0].State);

                Assert.Equal(1, indexes[1].Definition.Collections.Count);
                Assert.Equal("Users", indexes[1].Definition.Collections.Single());

                Assert.Equal(2, indexes[1].Definition.MapFields.Count);
                Assert.Equal("Count", indexes[1].Definition.MapFields["Count"].Name);
                Assert.Equal(AggregationOperation.Count, indexes[1].Definition.MapFields["Count"].As<AutoIndexField>().Aggregation);
                Assert.Equal("Sum", indexes[1].Definition.MapFields["Sum"].Name);
                Assert.Equal(AggregationOperation.Sum, indexes[1].Definition.MapFields["Sum"].As<AutoIndexField>().Aggregation);

                definition = indexes[0].Definition as AutoMapReduceIndexDefinition;

                Assert.NotNull(definition);

                Assert.Equal(1, definition.GroupByFields.Count);
                Assert.Equal("Location", definition.GroupByFields["Location"].Name);
                Assert.Equal(AutoFieldIndexing.Search | AutoFieldIndexing.Exact | AutoFieldIndexing.Default, definition.GroupByFields["Location"].Indexing);
                Assert.Equal(GroupByArrayBehavior.ByContent, definition.GroupByFields["Location"].GroupByArrayBehavior);

                Assert.Equal(IndexLockMode.Unlock, indexes[1].Definition.LockMode);
                Assert.Equal(IndexPriority.High, indexes[1].Definition.Priority);
                Assert.Equal(IndexState.Disabled, indexes[1].State);
            }
        }

        [Fact]
        public async Task CanGroupByNestedFieldAndAggregateOnCollection()
        {
            using (var db = CreateDocumentDatabase())
            using (var mri = AutoMapReduceIndex.CreateNew(new AutoMapReduceIndexDefinition(
                "Orders",
                new[]
                {
                    new AutoIndexField
                    {
                        Name = "Lines[].Quantity",
                        Aggregation = AggregationOperation.Sum,
                        Storage = FieldStorage.Yes
                    },
                    new AutoIndexField
                    {
                        Name = "Lines[].Price",
                        Aggregation = AggregationOperation.Sum,
                        Storage = FieldStorage.Yes
                    }
                },
                new[]
                {
                    new AutoIndexField
                    {
                        Name = "ShipTo.Country",
                        Storage = FieldStorage.Yes
                    },
                }), db))
            {
                CreateOrders(db, 5, new[] { "Poland", "Israel" });

                mri.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await mri.Query(new IndexQueryServerSide($"FROM INDEX '{mri.Name}' WHERE ShipTo.Country = 'Poland'"), context, OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);
                    var result = queryResult.Results[0].Data;

                    string location;
                    Assert.True(result.TryGet("ShipTo.Country", out location));
                    Assert.Equal("Poland", location);

                    var price = result["Lines[].Price"] as LazyNumberValue;

                    Assert.NotNull(price);

                    Assert.Equal(63.6, price, 1);

                    var quantity = result["Lines[].Quantity"];

                    Assert.Equal(9L, quantity);
                }
            }
        }

        [Fact]
        public void CanStoreAndReadReduceStats()
        {
            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(GetUsersCountByLocationIndexDefinition(), db))
            {
                index._indexStorage.UpdateStats(SystemTime.UtcNow, new IndexingRunStats
                {
                    ReduceAttempts = 1000,
                    ReduceSuccesses = 900,
                    ReduceErrors = 100,
                });

                var stats = index.GetStats();

                Assert.Equal(1000, stats.ReduceAttempts);
                Assert.Equal(900, stats.ReduceSuccesses);
                Assert.Equal(100, stats.ReduceErrors);
            }
        }

        public static void CreateUsers(DocumentDatabase db, long numberOfUsers, params string[] locations)
        {
            using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
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
                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                            {
                                [Constants.Documents.Metadata.Collection] = "Users"
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

        [Fact]
        public async Task CanUpdateByChangingValue()
        {
            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(new AutoMapReduceIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Age",
                    Aggregation = AggregationOperation.Sum,
                    Storage = FieldStorage.Yes
                }
            }, new[]
                    {
                new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes
                },
            }), db))
            {
                CreateUsers(db, 2, "Poland");

                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(41L, results[0].Data["Age"]);
                }

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        using (var doc = context.ReadObject(new DynamicJsonValue
                        {
                            ["Name"] = "modified",
                            ["Location"] = "Poland",
                            ["Age"] = 30,
                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                            {
                                [Constants.Documents.Metadata.Collection] = "Users"
                            }
                        }, "users/0"))
                        {
                            db.DocumentsStorage.Put(context, "users/0", null, doc);
                        }

                        tx.Commit();
                    }
                }

                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(51L, results[0].Data["Age"]);
                }
            }
        }

        [Fact]
        public async Task CanUpdateByChangingReduceKey()
        {
            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(new AutoMapReduceIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Age",
                    Aggregation = AggregationOperation.Sum,
                    Storage = FieldStorage.Yes
                }
            }, new[]
            {
                    new AutoIndexField
                    {
                        Name = "Location",
                        Storage = FieldStorage.Yes
                    },
            }), db))
            {
                CreateUsers(db, 2, "Poland");

                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(1, results.Count);

                    Assert.Equal("Poland", results[0].Data["Location"].ToString());
                    Assert.Equal(41L, results[0].Data["Age"]);
                }

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        using (var doc = context.ReadObject(new DynamicJsonValue
                        {
                            ["Name"] = "James",
                            ["Location"] = "Israel",
                            ["Age"] = 20,
                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                            {
                                [Constants.Documents.Metadata.Collection] = "Users"
                            }
                        }, "users/0"))
                        {
                            db.DocumentsStorage.Put(context, "users/0", null, doc);
                        }

                        tx.Commit();
                    }
                }

                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await index.Query(new IndexQueryServerSide("FROM Users ORDER BY Location"), context, OperationCancelToken.None);

                    var results = queryResult.Results;

                    Assert.Equal(2, results.Count);

                    Assert.Equal("Israel", results[0].Data["Location"].ToString());
                    Assert.Equal(20L, results[0].Data["Age"]);

                    Assert.Equal("Poland", results[1].Data["Location"].ToString());
                    Assert.Equal(21L, results[1].Data["Age"]);
                }
            }
        }

        [Fact]
        public async Task GroupByMultipleFields()
        {
            using (var db = CreateDocumentDatabase())
            using (var index = AutoMapReduceIndex.CreateNew(new AutoMapReduceIndexDefinition("Orders", new[]
            {
                new AutoIndexField
                {
                    Name = "Count",
                    Aggregation = AggregationOperation.Count,
                    Storage = FieldStorage.Yes
                }
            }, new[]
            {
                    new AutoIndexField
                    {
                        Name = "Employee",
                        Storage = FieldStorage.Yes
                    },
                    new AutoIndexField
                    {
                        Name = "Company",
                        Storage = FieldStorage.Yes
                    },
            }), db))
            {
                CreateOrders(db, 10, employees: new[] { "employees/1", "employees/2" }, companies: new[] { "companies/1", "companies/2", "companies/3" });

                index.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                {
                    var results = (await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}'"), context, OperationCancelToken.None)).Results;

                    context.CloseTransaction();

                    Assert.Equal(6, results.Count);
                }
                for (int i = 0; i < 6; i++)
                {
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
                    {
                        var employeeNumber = i % 2 + 1;
                        var companyNumber = i % 3 + 1;
                        var results = (await index.Query(new IndexQueryServerSide($"FROM INDEX '{index.Name}' WHERE Employee = 'employees/{employeeNumber}' AND Company = 'companies/{companyNumber}'")
                        {
                            Query = $"Employee:employees/{employeeNumber} AND Company:companies/{companyNumber}"
                        }, context, OperationCancelToken.None)).Results;

                        Assert.Equal(1, results.Count);

                        long expectedCount;

                        if ((employeeNumber == 1 && companyNumber == 2) || (employeeNumber == 2 && companyNumber == 3))
                            expectedCount = 1;
                        else
                            expectedCount = 2;

                        Assert.Equal(expectedCount, results[0].Data["Count"]);
                    }
                }
            }
        }

        private static void CreateOrders(DocumentDatabase db, int numberOfOrders, string[] countries = null, string[] employees = null, string[] companies = null)
        {
            using (var context = DocumentsOperationContext.ShortTermSingleUse(db))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    for (int i = 0; i < numberOfOrders; i++)
                    {
                        using (var doc = context.ReadObject(new DynamicJsonValue
                        {
                            ["Employee"] = employees?[i % employees.Length],
                            ["Company"] = companies?[i % companies.Length],
                            ["ShipTo"] = new DynamicJsonValue
                            {
                                ["Country"] = countries?[i % countries.Length],
                            },
                            ["Lines"] = new DynamicJsonArray
                            {
                                new DynamicJsonValue
                                {
                                    ["Price"] = 10.5,
                                    ["Quantity"] = 1
                                },
                                new DynamicJsonValue
                                {
                                    ["Price"] = 10.7,
                                    ["Quantity"] = 2
                                }
                            },
                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                            {
                                [Constants.Documents.Metadata.Collection] = "Orders"
                            }
                        }, $"orders/{i}"))
                        {
                            db.DocumentsStorage.Put(context, $"orders/{i}", null, doc);
                        }
                    }

                    tx.Commit();
                }
            }
        }

        private static AutoMapReduceIndexDefinition GetUsersCountByLocationIndexDefinition()
        {
            return new AutoMapReduceIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Count",
                    Aggregation = AggregationOperation.Count,
                    Storage = FieldStorage.Yes
                }
            }, new[]
            {
                new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes
                },
            });
        }
    }
}
