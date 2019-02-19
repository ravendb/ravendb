using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries;
using Raven.Server.Extensions;
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
                using (var index = MapIndex.CreateNew(new IndexDefinition()
                {
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                }, database))
                {
                    DocumentQueryResult queryResult;
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                            {
                                ["Name"] = "John",
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
                                ["Name"] = "Edward",
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = "Users"
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

                        queryResult =
                            await index.Query(new IndexQueryServerSide($"FROM '{index.Name}'"), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count);

                    }

                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        queryResult = await index.Query(new IndexQueryServerSide($"FROM '{index.Name}' WHERE Name = 'John'"), context, OperationCancelToken.None);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("users/1", queryResult.Results[0].Id);
                    }
                }
            }
        }

        [Fact]
        public async Task CanInheritConfiguration()
        {
            using (var database = CreateDocumentDatabase())
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Configuration =
                    {
                        {RavenConfiguration.GetKey(x => x.Indexing.MapTimeout), "33"}
                    }
                };

                var index = await database.IndexStore.CreateIndex(indexDefinition);
                Assert.NotNull(index);

                Assert.Equal(33, (int)index.Configuration.MapTimeout.AsTimeSpan.TotalSeconds);
                Assert.NotEqual(database.Configuration.Indexing.MapTimeout.AsTimeSpan, index.Configuration.MapTimeout.AsTimeSpan);
            }
        }

        [Fact]
        public async Task CanPersist()
        {

            IndexDefinition indexDefinition1, indexDefinition2;
            string dbName;


            using (CreatePersistentDocumentDatabase(NewDataPath(), out var database))
            {
                dbName = database.Name;

                indexDefinition1 = new IndexDefinition
                {
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Configuration =
                    {
                        { "TestKey", "TestValue" }
                    }
                };

                var index1 = await database.IndexStore.CreateIndex(indexDefinition1);
                Assert.NotNull(index1);

                indexDefinition2 = new IndexDefinition
                {
                    Name = "Users_ByAge",
                    Maps = { "from user in docs.Users select new { CustomAge = user.Age }" }
                };

                var index2 = await database.IndexStore.CreateIndex(indexDefinition2);
                Assert.NotNull(index2);

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(dbName);

                database = await GetDatabase(dbName);

                var indexes = database
                    .IndexStore
                    .GetIndexesForCollection("Users")
                    .OrderByDescending(x=>x.Name.Length)
                    .ToList();

                Assert.Equal(IndexType.Map, indexes[0].Type);
                Assert.Equal("Users_ByName", indexes[0].Name);
                Assert.Equal(1, indexes[0].Definition.Collections.Count);
                Assert.Equal("Users", indexes[0].Definition.Collections.Single());
                Assert.Equal(1, indexes[0].Definition.MapFields.Count);
                Assert.Contains("Name", indexes[0].Definition.MapFields.Keys);
                Assert.Equal(IndexLockMode.Unlock, indexes[0].Definition.LockMode);
                Assert.Equal(IndexPriority.Normal, indexes[0].Definition.Priority);
                Assert.True(indexDefinition1.Equals(indexes[0].GetIndexDefinition()));

                Assert.Equal(IndexType.Map, indexes[1].Type);
                Assert.Equal("Users_ByAge", indexes[1].Name);
                Assert.Equal(1, indexes[1].Definition.Collections.Count);
                Assert.Equal("Users", indexes[1].Definition.Collections.Single());
                Assert.Equal(1, indexes[1].Definition.MapFields.Count);
                Assert.Contains("CustomAge", indexes[1].Definition.MapFields.Keys);
                Assert.Equal(IndexLockMode.Unlock, indexes[1].Definition.LockMode);
                Assert.Equal(IndexPriority.Normal, indexes[1].Definition.Priority);
                Assert.True(indexDefinition2.Equals(indexes[1].GetIndexDefinition()));
            }
        }

        [Fact]
        public void IndexDefinitionSerialization()
        {
            var indexDefinition = new IndexDefinition();
#if FEATURE_TEST_INDEX
            indexDefinition.IsTestIndex = true;
#endif
            indexDefinition.LockMode = IndexLockMode.LockedIgnore;
            indexDefinition.Maps = new HashSet<string>
            {
                "a",
                "b"
            };
            indexDefinition.Name = "n1";
            indexDefinition.Reduce = "c";
            indexDefinition.Fields = new Dictionary<string, IndexFieldOptions>
            {
                {"f1", new IndexFieldOptions
                    {
                        Spatial = new SpatialOptions
                        {
                            Type = SpatialFieldType.Geography,
                            Units = SpatialUnits.Miles,
                            MinY = 3,
                            MinX = 5,
                            MaxY = 2,
                            MaxX = 5,
                            Strategy = SpatialSearchStrategy.QuadPrefixTree,
                            MaxTreeLevel = 2
                        },
                        Indexing = FieldIndexing.No,
                        Suggestions = true,
                        Storage = FieldStorage.Yes,
                        Analyzer = "a1",
                        TermVector = FieldTermVector.WithPositionsAndOffsets
                    }
                },
                {"f2", new IndexFieldOptions
                    {
                        Spatial = new SpatialOptions
                        {
                            Type = SpatialFieldType.Cartesian,
                            Units = SpatialUnits.Kilometers,
                            MinY = 5,
                            MinX = 2,
                            MaxY = 9,
                            MaxX = 3,
                            Strategy = SpatialSearchStrategy.BoundingBox,
                            MaxTreeLevel = 5
                        },
                        Indexing = FieldIndexing.Exact,
                        Suggestions = false,
                        Storage = FieldStorage.No,
                        Analyzer = "a2",
                        TermVector = FieldTermVector.WithPositions
                    }
                }
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var builder = indexDefinition.ToJson();
                using (var json = context.ReadObject(builder, nameof(IndexDefinition)))
                {
                    var newIndexDefinition = JsonDeserializationServer.IndexDefinition(json);

                    Assert.True(indexDefinition.Equals(newIndexDefinition));
                }
            }
        }

        [Fact]
        public void StalenessCalculationShouldWorkForAllDocsIndexes()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapIndex.CreateNew(new IndexDefinition()
                {
                    Name = "Index1",
                    Maps = { "from doc in docs select new { doc.Name }" },
                }, database))
                {
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                            {
                                ["Name"] = "John",
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "users/1", null, doc);
                            }

                            using (var doc = CreateDocument(context, "people/1", new DynamicJsonValue
                            {
                                ["Name"] = "Edward",
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = "People"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "people/1", null, doc);
                            }

                            tx.Commit();
                        }

                        using (context.OpenReadTransaction())
                        {
                            var isStale = index.IsStale(context);
                            Assert.True(isStale);
                        }

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);

                        Assert.Equal(2, batchStats.MapAttempts);
                        Assert.Equal(2, batchStats.MapSuccesses);
                        Assert.Equal(0, batchStats.MapErrors);

                        using (context.OpenReadTransaction())
                        {
                            var isStale = index.IsStale(context);
                            Assert.False(isStale);
                        }

                        using (var tx = context.OpenWriteTransaction())
                        {
                            database.DocumentsStorage.Delete(context, "people/1", null);

                            tx.Commit();
                        }

                        using (context.OpenReadTransaction())
                        {
                            var isStale = index.IsStale(context);
                            Assert.True(isStale);
                        }

                        batchStats = new IndexingRunStats();
                        scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);

                        using (context.OpenReadTransaction())
                        {
                            var isStale = index.IsStale(context);
                            Assert.False(isStale);
                        }
                    }
                }
            }
        }

        [Fact]
        public void NumberOfDocumentsAndTombstonesToProcessShouldBeCalculatedCorrectly()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = MapIndex.CreateNew(new IndexDefinition()
                {
                    Name = "Index1",
                    Maps = { "from doc in docs.Users select new { doc.Name }" },
                }, database))
                {
                    using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "users/1", new DynamicJsonValue
                            {
                                ["Name"] = "John",
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
                                ["Name"] = "Bob",
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "users/2", null, doc);
                            }

                            using (var doc = CreateDocument(context, "people/1", new DynamicJsonValue
                            {
                                ["Name"] = "Edward",
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = "People"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "people/1", null, doc);
                            }

                            tx.Commit();
                        }

                        IndexProgress progress;
                        using (context.OpenReadTransaction())
                        {
                            progress = index.GetProgress(context);
                        }

                        Assert.Equal(0, progress.Collections["Users"].LastProcessedDocumentEtag);
                        Assert.Equal(0, progress.Collections["Users"].LastProcessedTombstoneEtag);
                        Assert.Equal(2, progress.Collections["Users"].NumberOfDocumentsToProcess);
                        Assert.Equal(0, progress.Collections["Users"].NumberOfTombstonesToProcess);
                        Assert.Equal(2, progress.Collections["Users"].TotalNumberOfDocuments);
                        Assert.Equal(0, progress.Collections["Users"].TotalNumberOfTombstones);

                        var batchStats = new IndexingRunStats();
                        var scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);

                        using (context.OpenReadTransaction())
                        {
                            progress = index.GetProgress(context);
                        }

                        Assert.Equal(2, progress.Collections["Users"].LastProcessedDocumentEtag);
                        Assert.Equal(0, progress.Collections["Users"].LastProcessedTombstoneEtag);
                        Assert.Equal(0, progress.Collections["Users"].NumberOfDocumentsToProcess);
                        Assert.Equal(0, progress.Collections["Users"].NumberOfTombstonesToProcess);
                        Assert.Equal(2, progress.Collections["Users"].TotalNumberOfDocuments);
                        Assert.Equal(0, progress.Collections["Users"].TotalNumberOfTombstones);

                        using (var tx = context.OpenWriteTransaction())
                        {
                            database.DocumentsStorage.Delete(context, "users/1", null);

                            using (var doc = CreateDocument(context, "users/3", new DynamicJsonValue
                            {
                                ["Name"] = "George",
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "users/3", null, doc);
                            }

                            using (var doc = CreateDocument(context, "people/2", new DynamicJsonValue
                            {
                                ["Name"] = "Edward",
                                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                                {
                                    [Constants.Documents.Metadata.Collection] = "People"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "people/2", null, doc);
                            }

                            tx.Commit();
                        }

                        using (context.OpenReadTransaction())
                        {
                            progress = index.GetProgress(context);
                        }

                        Assert.Equal(2, progress.Collections["Users"].LastProcessedDocumentEtag);
                        Assert.Equal(0, progress.Collections["Users"].LastProcessedTombstoneEtag);
                        Assert.Equal(1, progress.Collections["Users"].NumberOfDocumentsToProcess);
                        Assert.Equal(1, progress.Collections["Users"].NumberOfTombstonesToProcess);
                        Assert.Equal(2, progress.Collections["Users"].TotalNumberOfDocuments);
                        Assert.Equal(1, progress.Collections["Users"].TotalNumberOfTombstones);

                        batchStats = new IndexingRunStats();
                        scope = new IndexingStatsScope(batchStats);
                        index.DoIndexingWork(scope, CancellationToken.None);

                        using (context.OpenReadTransaction())
                        {
                            progress = index.GetProgress(context);
                        }

                        Assert.Equal(5, progress.Collections["Users"].LastProcessedDocumentEtag);
                        Assert.Equal(4, progress.Collections["Users"].LastProcessedTombstoneEtag);
                        Assert.Equal(0, progress.Collections["Users"].NumberOfDocumentsToProcess);
                        Assert.Equal(0, progress.Collections["Users"].NumberOfTombstonesToProcess);
                        Assert.Equal(2, progress.Collections["Users"].TotalNumberOfDocuments);
                        Assert.Equal(1, progress.Collections["Users"].TotalNumberOfTombstones);
                    }
                }
            }
        }
    }
}
