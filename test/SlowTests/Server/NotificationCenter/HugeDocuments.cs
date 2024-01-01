using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.NotificationCenter
{
    public class HugeDocuments : ReplicationTestBase
    {
        public HugeDocuments(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Huge_document_hints_are_stored_and_can_be_read()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                // this tests write to storage
                database.HugeDocuments.AddIfDocIsHuge("orders/1-A", 10 * 1024 * 1024);

                // this tests merge with existing item
                database.HugeDocuments.AddIfDocIsHuge("orders/2-A", 20 * 1024 * 1024);
                database.HugeDocuments.AddIfDocIsHuge("orders/3-A", 30 * 1024 * 1024);
                database.HugeDocuments.AddIfDocIsHuge("orders/4-A", 40 * 1024 * 1024);

                database.HugeDocuments.UpdateHugeDocuments(null);

                Assert.True(database.ConfigurationStorage.NotificationsStorage.GetPerformanceHintCount() > 0);

                // now read directly from storage and verify
                using (database.ConfigurationStorage.NotificationsStorage.Read(Raven.Server.Documents.HugeDocuments.HugeDocumentsId, out var ntv))
                {
                    if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                    {
                        Assert.False(true, "Unable to read stored notification");
                    }
                    else
                    {
                        HugeDocumentsDetails details = (HugeDocumentsDetails)DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable(
                            typeof(HugeDocumentsDetails),
                            detailsJson,
                            Raven.Server.Documents.HugeDocuments.HugeDocumentsId);

                        Assert.NotNull(details);
                        Assert.Equal(4, details.HugeDocuments.Count);

                        var ids = details.HugeDocuments.Values.Select(x => x.Id).ToList();
                        Assert.Contains("orders/1-A", ids);
                        Assert.Contains("orders/2-A", ids);
                        Assert.Contains("orders/3-A", ids);
                        Assert.Contains("orders/4-A", ids);
                    }
                }
            }
        }

        [Fact]
        public async Task Huge_document_hints_are_available_for_empty_document_ids()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.PerformanceHints.HugeDocumentSize)] = "0";
                }
            }))
            {
                string documentId;

                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user, string.Empty);
                    session.SaveChanges();

                    documentId = user.Id;
                }

                var database = await GetDatabase(store.Database);
                database.HugeDocuments.UpdateHugeDocuments(null);

                Assert.True(database.ConfigurationStorage.NotificationsStorage.GetPerformanceHintCount() > 0);

                using (database.ConfigurationStorage.NotificationsStorage.Read(Raven.Server.Documents.HugeDocuments.HugeDocumentsId, out var ntv))
                {
                    if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                    {
                        Assert.False(true, "Unable to read stored notification");
                    }
                    else
                    {
                        HugeDocumentsDetails details = (HugeDocumentsDetails)DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable(
                            typeof(HugeDocumentsDetails),
                            detailsJson,
                            Raven.Server.Documents.HugeDocuments.HugeDocumentsId);

                        Assert.NotNull(details);
                        Assert.Equal(1, details.HugeDocuments.Count);

                        var id = details.HugeDocuments.Values.Select(x => x.Id).FirstOrDefault();
                        Assert.Equal(documentId, id);
                    }
                }
            }
        }


        [Fact]
        public async Task Huge_document_hints_are_available_for_patched_documents()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.PerformanceHints.HugeDocumentSize)] = "0";
                }
            }))
            {
                string documentId;

                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user, string.Empty);
                    session.SaveChanges();

                    documentId = user.Id;
                }

                var result = store.Operations.Send(new PatchOperation(documentId, null, new Raven.Client.Documents.Operations.PatchRequest
                {
                    Script = "put(null, this)",
                }));

                var database = await GetDatabase(store.Database);
                database.HugeDocuments.UpdateHugeDocuments(null);

                Assert.True(database.ConfigurationStorage.NotificationsStorage.GetPerformanceHintCount() > 0);

                using (database.ConfigurationStorage.NotificationsStorage.Read(Raven.Server.Documents.HugeDocuments.HugeDocumentsId, out var ntv))
                {
                    if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                    {
                        Assert.False(true, "Unable to read stored notification");
                    }
                    else
                    {
                        HugeDocumentsDetails details = (HugeDocumentsDetails)DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable(
                            typeof(HugeDocumentsDetails),
                            detailsJson,
                            Raven.Server.Documents.HugeDocuments.HugeDocumentsId);

                        Assert.NotNull(details);
                        Assert.Equal(2, details.HugeDocuments.Count);
                    }
                }
            }
        }

        // RavenDB-20129
        [RavenFact(RavenTestCategory.None)]
        public async Task ShouldUpdateNotificationAfterHugeDocumentDelete()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.PerformanceHints.HugeDocumentSize)] = "0"
            }))
            {
                string documentId = "users/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var database = await GetDatabase(store.Database);

                database.HugeDocuments.UpdateHugeDocuments(null);

                AssertHugeDocumentsDetails(database, documentId);

                using (var session = store.OpenSession())
                {
                    session.Delete(documentId);
                    session.SaveChanges();
                }

                AssertNotificationRemoved(database);
            }
        }

        // RavenDB-20129
        [RavenFact(RavenTestCategory.Replication)]
        public async Task ShouldUpdateNotificationAfterHugeDocumentDeleteFromReplication()
        {
            var options = new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.PerformanceHints.HugeDocumentSize)] = "0"
            };

            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                string documentId = "users/1";
                using (var session = store1.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                Assert.NotNull(await WaitForDocumentToReplicateAsync<User>(store2, documentId, 15_000));

                var database1 = await GetDatabase(store1.Database);
                var database2 = await GetDatabase(store2.Database);

                database1.HugeDocuments.UpdateHugeDocuments(null);
                database2.HugeDocuments.UpdateHugeDocuments(null);

                AssertHugeDocumentsDetails(database1, documentId);
                AssertHugeDocumentsDetails(database2, documentId);

                using (var session = store1.OpenSession())
                {
                    session.Delete(documentId);
                    session.SaveChanges();
                }

                Assert.True(WaitForDocumentDeletion(store2, documentId));

                AssertNotificationRemoved(database1);
                AssertNotificationRemoved(database2);
            }
        }

        private void AssertHugeDocumentsDetails(DocumentDatabase database, string documentId)
        {
            Assert.True(database.ConfigurationStorage.NotificationsStorage.GetPerformanceHintCount() > 0);

            using (database.ConfigurationStorage.NotificationsStorage.Read(Raven.Server.Documents.HugeDocuments.HugeDocumentsId, out var ntv))
            {
                if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                {
                    Assert.False(true, "Unable to read stored notification");
                }
                else
                {
                    HugeDocumentsDetails details = (HugeDocumentsDetails)DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable(
                        typeof(HugeDocumentsDetails),
                        detailsJson,
                        Raven.Server.Documents.HugeDocuments.HugeDocumentsId);

                    Assert.NotNull(details);
                    Assert.Equal(1, details.HugeDocuments.Count);

                    var id = details.HugeDocuments.Values.Select(x => x.Id).FirstOrDefault();
                    Assert.Equal(documentId, id);
                }
            }
        }

        private void AssertNotificationRemoved(DocumentDatabase database)
        {
            using (database.ConfigurationStorage.NotificationsStorage.Read(Raven.Server.Documents.HugeDocuments.HugeDocumentsId, out var ntv))
            {
                Assert.True(ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null);
            }
        }
    }
}
