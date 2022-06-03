using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4840 : RavenTestBase
    {
        public RavenDB_4840(ITestOutputHelper output) : base(output)
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

                Assert.True(database.NotificationCenter.Storage.GetPerformanceHintCount() > 0);

                // now read directly from storage and verify
                using (database.NotificationCenter.Storage.Read(HugeDocuments.HugeDocumentsId, out var ntv))
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
                            HugeDocuments.HugeDocumentsId);

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
    }
}
