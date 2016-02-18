using System;
using System.Diagnostics;
using System.IO;
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
using Raven.Server.Utils;
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
                var index = AutoIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name", SortOptions.String) }), database);
                index.Dispose();

                Assert.Throws<ObjectDisposedException>(() => index.Dispose());
                Assert.Throws<ObjectDisposedException>(() => index.Execute(CancellationToken.None));
                Assert.Throws<ObjectDisposedException>(() => index.Query(new IndexQuery(), null, CancellationToken.None));

                index = AutoIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name", SortOptions.String) }), database);
                index.Execute(CancellationToken.None);
                index.Dispose();

                using (var cts = new CancellationTokenSource())
                {
                    index = AutoIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name", SortOptions.String) }), database);
                    index.Execute(cts.Token);

                    cts.Cancel();

                    index.Dispose();
                }
            }
        }

        [Fact]
        public void CanDispose()
        {
            DocumentDatabase database = null;
            try
            {
                using (database = CreateDocumentDatabase(runInMemory: false))
                {
                    Assert.Equal(1, database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name1", SortOptions.String) })));
                    Assert.Equal(2, database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name2", SortOptions.String) })));
                }
            }
            finally
            {
                if (database != null)
                    IOExtensions.DeleteDirectory(database.Configuration.Core.DataDirectory);
            }
        }

        [Fact]
        public void CanPersist()
        {
            DocumentDatabase database = null;
            try
            {
                using (database = CreateDocumentDatabase(runInMemory: false))
                {
                    Assert.Equal(1, database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name1", SortOptions.String, true) })));
                    Assert.Equal(2, database.IndexStore.CreateIndex(new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name2", SortOptions.Float, false) })));
                }

                using (database = CreateDocumentDatabase(runInMemory: false))
                {
                    Assert.True(SpinWait.SpinUntil(() => database.IndexStore.GetIndex(2) != null, TimeSpan.FromSeconds(5000)));

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
            finally
            {
                if (database != null)
                    IOExtensions.DeleteDirectory(database.Configuration.Core.DataDirectory);
            }
        }

        [Fact]
        public void SimpleIndexing()
        {
            using (var database = CreateDocumentDatabase())
            {
                using (var index = AutoIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name", SortOptions.String) }), database))
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

                        index.Execute(CancellationToken.None);

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
            Assert.True(SpinWait.SpinUntil(() => index.GetLastMappedEtag() == etag, timeout));
        }

        private static void WaitForTombstone(Index index, long etag)
        {
            var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);
            Assert.True(SpinWait.SpinUntil(() => index.GetLastTombstoneEtag() == etag, timeout));
        }

        private static BlittableJsonReaderObject CreateDocument(MemoryOperationContext context, string key, DynamicJsonValue value)
        {
            return context.ReadObject(value, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

        private static DocumentDatabase CreateDocumentDatabase([CallerMemberName] string caller = null, bool runInMemory = true)
        {
            var name = caller ?? Guid.NewGuid().ToString("N");

            var configuration = new RavenConfiguration();
            configuration.Initialize();
            configuration.Core.RunInMemory = runInMemory;
            configuration.Core.DataDirectory = Path.Combine(configuration.Core.DataDirectory, name);

            var documentDatabase = new DocumentDatabase(name, configuration);
            documentDatabase.Initialize();

            return documentDatabase;
        }
    }
}