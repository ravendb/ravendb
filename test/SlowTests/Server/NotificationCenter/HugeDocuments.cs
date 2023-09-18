using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.NotificationCenter
{
    public class HugeDocuments : RavenTestBase
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
    }
}
