using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Config;
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
            using (var database = CreateDocumentDatabase())
            {
                var index = AutoMapIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                } }), database);
                index.Dispose();

                Assert.Throws<ObjectDisposedException>(() => index.Dispose());
                Assert.Throws<ObjectDisposedException>(() => index.Execute());
                Assert.Throws<ObjectDisposedException>(() => index.Query(new IndexQuery(), null, CancellationToken.None));

                index = AutoMapIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                } }), database);
                index.Execute();
                index.Dispose();

                using (var cts = new CancellationTokenSource())
                {
                    index = AutoMapIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new IndexField
                    {
                        Name = "Name",
                        Highlighted = false,
                        Storage = FieldStorage.No
                    } }), database);
                    index.Execute();

                    cts.Cancel();

                    index.Dispose();
                }
            }
        }

        [Fact]
        public void CanDispose()
        {
            using (var database = CreateDocumentDatabase(runInMemory: false))
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
            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
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
                    SortOption = SortOptions.Float
                };
                Assert.Equal(2, database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { name2 })));
            }

            using (var database = CreateDocumentDatabase(runInMemory: false, dataDirectory: path))
            {
                Assert.True(SpinWait.SpinUntil(() => database.IndexStore.GetIndex(2) != null, TimeSpan.FromSeconds(15)));

                var indexes = database
                    .IndexStore
                    .GetIndexesForCollection("Users")
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
                Assert.Equal(SortOptions.Float, indexes[1].Definition.MapFields[0].SortOption);
                Assert.False(indexes[1].Definition.MapFields[0].Highlighted);
            }
        }

        [Fact]
        public void SimpleIndexing()
        {
            using (var database = CreateDocumentDatabase())
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

                        index.Execute();

                        WaitForIndexMap(index, 2);

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

                        WaitForIndexMap(index, 3);

                        using (var tx = context.OpenWriteTransaction())
                        {
                            database.DocumentsStorage.Delete(context, "key/1", null);

                            tx.Commit();
                        }

                        WaitForTombstone(index, 4);
                    }
                }
            }
        }

        private static void WaitForIndexMap(Index index, long etag)
        {
            var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);
            Assert.True(SpinWait.SpinUntil(() => index.GetLastMappedEtags().Values.Min() == etag, timeout));
        }

        private static void WaitForTombstone(Index index, long etag)
        {
            var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);
            Assert.True(SpinWait.SpinUntil(() => index.GetLastTombstoneEtags().Values.Min() == etag, timeout));
        }

        private static BlittableJsonReaderObject CreateDocument(MemoryOperationContext context, string key, DynamicJsonValue value)
        {
            return context.ReadObject(value, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

        private DocumentDatabase CreateDocumentDatabase([CallerMemberName] string caller = null, bool runInMemory = true, string dataDirectory = null)
        {
            var name = caller ?? Guid.NewGuid().ToString("N");

            if (string.IsNullOrEmpty(dataDirectory) == false)
                PathsToDelete.Add(dataDirectory);
            else
                dataDirectory = NewDataPath(name);

            var configuration = new RavenConfiguration();
            configuration.Initialize();
            configuration.Core.RunInMemory = runInMemory;
            configuration.Core.DataDirectory = dataDirectory;

            var documentDatabase = new DocumentDatabase(name, configuration);
            documentDatabase.Initialize();

            return documentDatabase;
        }
    }
}