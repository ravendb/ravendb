using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15215 : RavenTestBase
    {
        public RavenDB_15215(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanReduceCountersMetadataWhenLoadingDocumentsPage()
        {
            using var store = GetDocumentStore();
            using (var session = store.OpenSession())
            {
                session.Store(new Order { Freight = 1 }, "order/1");
                session.CountersFor("order/1").Increment("counter");
                session.SaveChanges();
            }
            var json = await store.GetRequestExecutor().HttpClient.GetStringAsync($"{store.Urls.First()}/databases/{store.Database}/studio/collections/preview");
            Assert.Contains(nameof(DocumentFlags.HasCounters), json);
            Assert.DoesNotContain(Constants.Documents.Metadata.Counters, json);
        }

        [Fact]
        public async Task CanReduceAttachmentsMetadataWhenLoadingDocumentsPage()
        {
            using var store = GetDocumentStore();
            using (var session = store.OpenSession())
            using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
            {
                session.Store(new Order { Freight = 1 }, "order/1");
                session.SaveChanges();
                var result = store.Operations.Send(new PutAttachmentOperation("order/1", "name", profileStream, "image/png"));
            }
            var json = await store.GetRequestExecutor().HttpClient.GetStringAsync($"{store.Urls.First()}/databases/{store.Database}/studio/collections/preview");
            Assert.Contains(nameof(DocumentFlags.HasAttachments), json);
            Assert.DoesNotContain(Constants.Documents.Metadata.Attachments, json);
        }

        [Fact]
        public async Task CanReduceTimeSeriesMetadataWhenLoadingDocumentsPage()
        {
            using var store = GetDocumentStore();
            using (var session = store.OpenSession())
            {
                session.Store(new Order { Freight = 1 }, "order/1");
                var t = session.TimeSeriesFor("order/1", "Heartrate");
                t.Append(DateTime.MaxValue, new[] { 59d }, "watches/fitbit");
                session.SaveChanges();
            }
            var json = await store.GetRequestExecutor().HttpClient.GetStringAsync($"{store.Urls.First()}/databases/{store.Database}/studio/collections/preview");
            Assert.Contains(nameof(DocumentFlags.HasTimeSeries), json);
            Assert.DoesNotContain(Constants.Documents.Metadata.TimeSeries, json);
        }

        [Fact]
        public async Task CanReduceMetadataWhenLoadingDocumentsPage()
        {
            using var store = GetDocumentStore();
            using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
            using (var session = store.OpenSession())
            {
                session.Store(new Order { Freight = 1 }, "order/1");
                session.CountersFor("order/1").Increment("counter");
                var t = session.TimeSeriesFor("order/1", "Heartrate");
                t.Append(DateTime.MaxValue, new[] { 59d }, "watches/fitbit");
                session.SaveChanges();
                var result = store.Operations.Send(new PutAttachmentOperation("order/1", "name", profileStream, "image/png"));
            }

            var json = await store.GetRequestExecutor().HttpClient.GetStringAsync($"{store.Urls.First()}/databases/{store.Database}/studio/collections/preview");
            Assert.Contains(nameof(DocumentFlags.HasCounters), json);
            Assert.Contains(nameof(DocumentFlags.HasAttachments), json);
            Assert.Contains(nameof(DocumentFlags.HasTimeSeries), json);
            Assert.DoesNotContain(Constants.Documents.Metadata.Counters, json);
            Assert.DoesNotContain(Constants.Documents.Metadata.Attachments, json);
            Assert.DoesNotContain(Constants.Documents.Metadata.TimeSeries, json);
        }
    }
}
