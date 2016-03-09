using System;
using System.IO;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core;

using Xunit;
using Constants = Raven.Abstractions.Data.Constants;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicIndexing : RavenTestBase
    {
        [Fact]
        public void CheckDispose()
        {
            using (var database = LowLevel_CreateDocumentDatabase())
            {
                var index = AutoMapIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                } }), database);
                index.Dispose();

                index.Dispose();// can dispose twice

                Assert.Throws<ObjectDisposedException>(() => index.Start());
                Assert.Throws<ObjectDisposedException>(() => index.Query(new IndexQuery(), null, CancellationToken.None));

                index = AutoMapIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                } }), database);
                index.Start();
                index.Dispose();

                using (var cts = new CancellationTokenSource())
                {
                    index = AutoMapIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new IndexField
                    {
                        Name = "Name",
                        Highlighted = false,
                        Storage = FieldStorage.No
                    } }), database);
                    index.Start();

                    cts.Cancel();

                    index.Dispose();
                }
            }
        }

        [Fact]
        public void CanDispose()
        {
            using (var database = LowLevel_CreateDocumentDatabase(runInMemory: false))
            {
                Assert.Equal(1, database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { new IndexField
                {
                    Name = "Name1",
                    Highlighted = false,
                    Storage = FieldStorage.No
                } })));
                Assert.Equal(2, database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { new IndexField
                {
                    Name = "Name2",
                    Highlighted = false,
                    Storage = FieldStorage.No
                } })));
            }
        }

        [Fact]
        public void CanPersist()
        {
            var path = NewDataPath();
            using (var database = LowLevel_CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                var name1 = new IndexField
                {
                    Name = "Name1",
                    Highlighted = true,
                    Storage = FieldStorage.No,
                    SortOption = SortOptions.String
                };
                Assert.Equal(1, database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { name1 })));
                var name2 = new IndexField
                {
                    Name = "Name2",
                    Highlighted = false,
                    Storage = FieldStorage.No,
                    SortOption = SortOptions.NumericDefault
                };
                Assert.Equal(2, database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { name2 })));
            }

            using (var database = LowLevel_CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                Assert.True(SpinWait.SpinUntil(() => database.IndexStore.GetIndex(2) != null, TimeSpan.FromSeconds(15)));

                var indexes = database
                    .IndexStore
                    .GetIndexesForCollection("Users")
                    .OrderBy(x=>x.IndexId)
                    .ToList();

                Assert.Equal(2, indexes.Count);

                Assert.Equal(1, indexes[0].IndexId);
                Assert.Equal(1, indexes[0].Definition.Collections.Length);
                Assert.Equal("Users", indexes[0].Definition.Collections[0]);
                Assert.Equal(1, indexes[0].Definition.MapFields.Length);
                Assert.Equal("Name1", indexes[0].Definition.MapFields[0].Name);
                Assert.Equal(SortOptions.String, indexes[0].Definition.MapFields[0].SortOption);
                Assert.True(indexes[0].Definition.MapFields[0].Highlighted);

                Assert.Equal(2, indexes[1].IndexId);
                Assert.Equal(1, indexes[1].Definition.Collections.Length);
                Assert.Equal("Users", indexes[1].Definition.Collections[0]);
                Assert.Equal(1, indexes[1].Definition.MapFields.Length);
                Assert.Equal("Name2", indexes[1].Definition.MapFields[0].Name);
                Assert.Equal(SortOptions.NumericDefault, indexes[1].Definition.MapFields[0].SortOption);
                Assert.False(indexes[1].Definition.MapFields[0].Highlighted);
            }
        }

        [Fact]
        public void CanDelete()
        {
            using (var database = LowLevel_CreateDocumentDatabase())
                CanDelete(database);

            var path = NewDataPath();
            using (var database = LowLevel_CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
                CanDelete(database);
        }

        private static void CanDelete(DocumentDatabase database)
        {
            var index1 =
                database.IndexStore.CreateIndex(
                    new AutoIndexDefinition("Users", new[] { new IndexField { Name = "Name1" } }));
            var path1 = Path.Combine(database.Configuration.Indexing.IndexStoragePath, index1.ToString());

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path1));

            var index2 =
                database.IndexStore.CreateIndex(
                    new AutoIndexDefinition("Users", new[] { new IndexField { Name = "Name2" } }));
            var path2 = Path.Combine(database.Configuration.Indexing.IndexStoragePath, index2.ToString());

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path2));

            Assert.Equal(2, database.IndexStore.GetIndexesForCollection("Users").Count());

            database.IndexStore.DeleteIndex(index1);

            Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path1) == false, TimeSpan.FromSeconds(5)));

            var indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(1, indexes.Count);
            Assert.Equal(index2, indexes[0].IndexId);

            database.IndexStore.DeleteIndex(index2);

            Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path2) == false, TimeSpan.FromSeconds(5)));

            indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(0, indexes.Count);
        }

        [Fact]
        public void CanReset()
        {
            using (var database = LowLevel_CreateDocumentDatabase())
                CanReset(database);

            var path = NewDataPath();
            using (var database = LowLevel_CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
                CanReset(database);
        }

        private static void CanReset(DocumentDatabase database)
        {
            var index1 =
                database.IndexStore.CreateIndex(
                    new AutoIndexDefinition("Users", new[] { new IndexField { Name = "Name1" } }));
            var path1 = Path.Combine(database.Configuration.Indexing.IndexStoragePath, index1.ToString());

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path1));

            var index2 =
                database.IndexStore.CreateIndex(
                    new AutoIndexDefinition("Users", new[] { new IndexField { Name = "Name2" } }));
            var path2 = Path.Combine(database.Configuration.Indexing.IndexStoragePath, index2.ToString());

            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path2));

            Assert.Equal(2, database.IndexStore.GetIndexesForCollection("Users").Count());

            var index3 = database.IndexStore.ResetIndex(index1);
            var path3 = Path.Combine(database.Configuration.Indexing.IndexStoragePath, index3.ToString());

            Assert.NotEqual(index3, index1);
            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path3));

            Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path1) == false, TimeSpan.FromSeconds(5)));

            var indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(2, indexes.Count);

            var index4 = database.IndexStore.ResetIndex(index2);
            var path4 = Path.Combine(database.Configuration.Indexing.IndexStoragePath, index4.ToString());

            Assert.NotEqual(index4, index2);
            if (database.Configuration.Core.RunInMemory == false)
                Assert.True(Directory.Exists(path4));

            Assert.True(SpinWait.SpinUntil(() => Directory.Exists(path2) == false, TimeSpan.FromSeconds(5)));

            indexes = database.IndexStore.GetIndexesForCollection("Users").ToList();

            Assert.Equal(2, indexes.Count);
        }

        [Fact]
        public void SimpleIndexing()
        {
            using (var database = LowLevel_CreateDocumentDatabase())
            {
                using (var index = AutoMapIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                } }), database))
                {
                    using (var context = new DocumentsOperationContext(new UnmanagedBuffersPool(string.Empty), database))
                    {
                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "key/1", new DynamicJsonValue
                            {
                                ["Name"] = "John",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "key/1", null, doc);
                            }

                            using (var doc = CreateDocument(context, "key/2", new DynamicJsonValue
                            {
                                ["Name"] = "Edward",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "key/2", null, doc);
                            }

                            tx.Commit();
                        }

                        index.DoIndexingWork(CancellationToken.None);
                        Assert.Equal(2, index.GetLastMappedEtagsForDebug().Values.Min());

                        using (var tx = context.OpenWriteTransaction())
                        {
                            using (var doc = CreateDocument(context, "key/3", new DynamicJsonValue
                            {
                                ["Name"] = "William",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                database.DocumentsStorage.Put(context, "key/3", null, doc);
                            }

                            tx.Commit();
                        }

                        index.DoIndexingWork(CancellationToken.None);
                        Assert.Equal(3, index.GetLastMappedEtagsForDebug().Values.Min());

                        using (var tx = context.OpenWriteTransaction())
                        {
                            database.DocumentsStorage.Delete(context, "key/1", null);

                            tx.Commit();
                        }

                        index.DoIndexingWork(CancellationToken.None);

                        Assert.Equal(4, index.GetLastTombstoneEtagsForDebug().Values.Min());
                    }
                }
            }
        }

        private static BlittableJsonReaderObject CreateDocument(MemoryOperationContext context, string key, DynamicJsonValue value)
        {
            return context.ReadObject(value, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

    }
}