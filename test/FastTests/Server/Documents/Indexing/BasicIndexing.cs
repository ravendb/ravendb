using System;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Tests.Core;

using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class BasicIndexing : RavenTestBase
    {
        [Fact]
        public void CheckDispose()
        {
            var notifications = new DatabaseNotifications();
            var indexingConfiguration = new IndexingConfiguration(() => true, () => null);

            using (var storage = CreateDocumentsStorage(notifications))
            {
                var index = AutoIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name", SortOptions.String) }), storage, indexingConfiguration, notifications);
                index.Dispose();

                Assert.Throws<ObjectDisposedException>(() => index.Dispose());
                Assert.Throws<ObjectDisposedException>(() => index.Execute(CancellationToken.None));
                Assert.Throws<ObjectDisposedException>(() => index.Query(new IndexQuery()));

                index = AutoIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name", SortOptions.String) }), storage, indexingConfiguration, notifications);
                index.Execute(CancellationToken.None);
                index.Dispose();

                using (var cts = new CancellationTokenSource())
                {
                    index = AutoIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name", SortOptions.String) }), storage, indexingConfiguration, notifications);
                    index.Execute(cts.Token);

                    cts.Cancel();

                    index.Dispose();
                }
            }
        }

        [Fact]
        public void SimpleIndexing()
        {
            var notifications = new DatabaseNotifications();
            var indexingConfiguration = new IndexingConfiguration(() => true, () => null);

            using (var storage = CreateDocumentsStorage(notifications))
            {
                using (var index = AutoIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name", SortOptions.String) }), storage, indexingConfiguration, notifications))
                {
                    using (var context = new RavenOperationContext(new UnmanagedBuffersPool(string.Empty))
                    {
                        Environment = storage.Environment
                    })
                    {
                        using (var tx = context.Environment.WriteTransaction())
                        {
                            context.Transaction = tx;

                            using (var doc =  CreateDocumentAsync(context, "key/1", new DynamicJsonValue
                            {
                                ["Name"] = "John",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                storage.Put(context, "key/1", null, doc);
                            }

                            using (var doc =  CreateDocumentAsync(context, "key/2", new DynamicJsonValue
                            {
                                ["Name"] = "Edward",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                storage.Put(context, "key/2", null, doc);
                            }

                            tx.Commit();
                        }

                        index.Execute(CancellationToken.None);

                        Assert.True(SpinWait.SpinUntil(() => index.GetLastMappedEtag() == 2, TimeSpan.FromSeconds(15)));

                        using (var tx = context.Environment.WriteTransaction())
                        {
                            context.Transaction = tx;

                            using (var doc = CreateDocumentAsync(context, "key/3", new DynamicJsonValue
                            {
                                ["Name"] = "William",
                                [Constants.Metadata] = new DynamicJsonValue
                                {
                                    [Constants.RavenEntityName] = "Users"
                                }
                            }))
                            {
                                storage.Put(context, "key/3", null, doc);
                            }

                            tx.Commit();
                        }

                        Assert.True(SpinWait.SpinUntil(() => index.GetLastMappedEtag() == 3, TimeSpan.FromSeconds(15)));
                    }
                }
            }
        }

        private static BlittableJsonReaderObject CreateDocumentAsync(RavenOperationContext context, string key, DynamicJsonValue value)
        {
            return context.ReadObject(value, key, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
        }

        private static DocumentsStorage CreateDocumentsStorage(DatabaseNotifications notifications)
        {
            var storage = new DocumentsStorage("Test", new RavenConfiguration { Core = { RunInMemory = true } }, notifications);
            storage.Initialize();

            return storage;
        }
    }
}