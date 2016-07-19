using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Blittable;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries;
using Raven.Server.Extensions;
using Raven.Server.Json;
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

                        var queryResult = await index.Query(new IndexQueryServerSide(), context, OperationCancelToken.None);

                        Assert.Equal(2, queryResult.Results.Count);

                        context.Reset();

                        queryResult = await index.Query(new IndexQueryServerSide() { Query = "Name:John" }, context, OperationCancelToken.None);

                        Assert.Equal(1, queryResult.Results.Count);
                        Assert.Equal("users/1", queryResult.Results[0].Key);
                    }
                }
            }
        }

        [Fact]
        public void CanPersist()
        {
            var path = NewDataPath();
            IndexDefinition indexDefinition1, indexDefinition2;
            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                indexDefinition1 = new IndexDefinition
                {
                    Name = "Users_ByName",
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map
                };
                Assert.Equal(1, database.IndexStore.CreateIndex(indexDefinition1));

                indexDefinition2 = new IndexDefinition
                {
                    Name = "Users_ByAge",
                    Maps = { "from user in docs.Users select new { CustomAge = user.Age }" },
                };
                Assert.Equal(2, database.IndexStore.CreateIndex(indexDefinition2));
            }

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path, modifyConfiguration: configuration => configuration.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened = true))
            {
                var indexes = database
                    .IndexStore
                    .GetIndexesForCollection("Users")
                    .OrderBy(x => x.IndexId)
                    .ToList();

                Assert.Equal(1, indexes[0].IndexId);
                Assert.Equal(IndexType.Map, indexes[0].Type);
                Assert.Equal("Users_ByName", indexes[0].Name);
                Assert.Equal(1, indexes[0].Definition.Collections.Length);
                Assert.Equal("Users", indexes[0].Definition.Collections[0]);
                Assert.Equal(1, indexes[0].Definition.MapFields.Count);
                Assert.Contains("Name", indexes[0].Definition.MapFields.Keys);
                Assert.Equal(IndexLockMode.Unlock, indexes[0].Definition.LockMode);
                Assert.Equal(IndexingPriority.Normal, indexes[0].Priority);
                Assert.True(indexes[0].Definition.Equals(indexDefinition1, ignoreFormatting: true, ignoreMaxIndexOutputs: false));
                Assert.True(indexDefinition1.Equals(indexes[0].GetIndexDefinition(), compareIndexIds: false, ignoreFormatting: false, ignoreMaxIndexOutput: false));

                Assert.Equal(2, indexes[1].IndexId);
                Assert.Equal(IndexType.Map, indexes[1].Type);
                Assert.Equal("Users_ByAge", indexes[1].Name);
                Assert.Equal(1, indexes[1].Definition.Collections.Length);
                Assert.Equal("Users", indexes[1].Definition.Collections[0]);
                Assert.Equal(1, indexes[1].Definition.MapFields.Count);
                Assert.Contains("CustomAge", indexes[1].Definition.MapFields.Keys);
                Assert.Equal(IndexLockMode.Unlock, indexes[1].Definition.LockMode);
                Assert.Equal(IndexingPriority.Normal, indexes[1].Priority);
                Assert.True(indexes[1].Definition.Equals(indexDefinition2, ignoreFormatting: true, ignoreMaxIndexOutputs: false));
                Assert.True(indexDefinition2.Equals(indexes[1].GetIndexDefinition(), compareIndexIds: false, ignoreFormatting: false, ignoreMaxIndexOutput: false));
            }
        }

        [Fact]
        public void IndexDefinitionSerialization()
        {
            var indexDefinition = new IndexDefinition();
            indexDefinition.IsTestIndex = true;
            indexDefinition.LockMode = IndexLockMode.LockedIgnore;
            indexDefinition.Maps = new HashSet<string>
            {
                "a",
                "b"
            };
            indexDefinition.MaxIndexOutputsPerDocument = 5;
            indexDefinition.Name = "n1";
            indexDefinition.Reduce = "c";
            indexDefinition.Type = IndexType.MapReduce;
            indexDefinition.IndexId = 3;
            indexDefinition.IndexVersion = 2;
            indexDefinition.IsSideBySideIndex = true;
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
                        Sort = SortOptions.NumericDouble,
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
                        Indexing = FieldIndexing.NotAnalyzed,
                        Suggestions = false,
                        Storage = FieldStorage.No,
                        Analyzer = "a2",
                        Sort = SortOptions.NumericDefault,
                        TermVector = FieldTermVector.WithPositions
                    }
                }
            };

            using (var context = new JsonOperationContext(new UnmanagedBuffersPool(string.Empty)))
            {
                var builder = indexDefinition.ToJson();
                using (var json = context.ReadObject(builder, nameof(IndexDefinition)))
                {
                    var newIndexDefinition = JsonDeserialization.IndexDefinition(json);

                    Assert.True(indexDefinition.Equals(newIndexDefinition, compareIndexIds: true, ignoreFormatting: false, ignoreMaxIndexOutput: false));
                }
            }
        }
    }
}