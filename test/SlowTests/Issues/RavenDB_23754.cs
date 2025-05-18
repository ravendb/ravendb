using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using FastTests;
using Raven.Client.Documents.Operations;
using SlowTests.Core.Utils.Entities;
using SlowTests.Core.Utils.Entities.Faceted;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23754 : RavenTestBase
    {
        public RavenDB_23754(ITestOutputHelper output) : base(output)
        {
        }

        private const string UsersId = "Users/1";
        private const string OrdersId = "Orders/1";

        [RavenFact(RavenTestCategory.ClientApi)]
        public void DebugPackage_Should_Contain_Collections_Stats_Detailed()
        {
            using var store = GetDocumentStore();
            var dbName = store.Database;

            using (var s = store.OpenSession())
            {
                s.Store(new User(), UsersId);
                s.Store(new Order(), OrdersId);
                s.SaveChanges();
            }

            var baseUrl = $"{store.Urls.Single().TrimEnd('/')}";
            using var http = new HttpClient();
            var resp = http.GetAsync($"{baseUrl}/admin/debug/info-package").Result;
            resp.EnsureSuccessStatusCode();

            using var zipStream = resp.Content.ReadAsStreamAsync().Result;
            using var zip = new ZipArchive(zipStream);

            var entryName = $"{dbName}/collections.stats.detailed.json";
            var detailedEntry = zip.Entries.SingleOrDefault(e => e.FullName == entryName);
            Assert.NotNull(detailedEntry);

            using var reader = new StreamReader(detailedEntry.Open());
            var json = reader.ReadToEnd();
            Assert.Contains("\"Users\"", json);
            Assert.Contains("\"Orders\"", json);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void GetCollectionStatsTests()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "user1" }, "users/1");
                    session.Store(new User() { Name = "user2" }, "users/2");
                    session.Store(new User() { Name = "user3" }, "users/3");
                    session.Store(new Company() { Name = "com1" }, "com/1");
                    session.Store(new Company() { Name = "com2" }, "com/2");
                    session.Store(new Address() { City = "city1" }, "add/1");

                    session.SaveChanges();

                    session.Advanced.Revisions.ForceRevisionCreationFor("users/1");
                    session.Advanced.Revisions.ForceRevisionCreationFor("com/1");

                    session.Delete("users/3");

                    session.Advanced.Attachments.Store(
                        "users/1", "hello.txt",
                        new MemoryStream(Encoding.UTF8.GetBytes("hello")));

                    session.CountersFor("users/1").Increment("likes", 5);
                    session.CountersFor("com/1").Increment("views", 10);

                    var tsTime = DateTime.UtcNow;
                    var ts = session.TimeSeriesFor("users/1", "heartrate");
                    ts.Append(tsTime, 72, "wrist");

                    ts.Delete(tsTime.AddMinutes(-5), tsTime.AddMinutes(-1));

                    session.SaveChanges();
                }

                var collectionStats = store.Maintenance.Send(new GetCollectionStatisticsOperation());

                Assert.Equal(3, collectionStats.Collections.Count);
                Assert.Equal(5, collectionStats.CountOfDocuments);
                Assert.Equal(0, collectionStats.CountOfConflicts);

                var detailedCollectionStats = store.Maintenance.Send(new GetDetailedCollectionStatisticsOperation());

                Assert.Equal(3, detailedCollectionStats.Collections.Count);
                Assert.Equal(5, detailedCollectionStats.CountOfDocuments);
                Assert.Equal(0, detailedCollectionStats.CountOfConflicts);
                Assert.Equal(2, detailedCollectionStats.CountOfRevisionDocuments);
                Assert.Equal(1, detailedCollectionStats.CountOfTombstones);
                Assert.Equal(1, detailedCollectionStats.CountOfAttachments);
                Assert.Equal(2, detailedCollectionStats.CountOfCounterEntries);
                Assert.Equal(1, detailedCollectionStats.CountOfTimeSeriesSegments);
                Assert.Equal(1, detailedCollectionStats.CountOfTimeSeriesDeletedRanges);
                Assert.Equal(0, detailedCollectionStats.CountOfDocumentsConflicts);
                Assert.Equal(2, detailedCollectionStats.Collections["Users"].CountOfDocuments);
                Assert.Equal(2, detailedCollectionStats.Collections["Companies"].CountOfDocuments);
                Assert.Equal(1, detailedCollectionStats.Collections["Addresses"].CountOfDocuments);
            }
        }
    }
}


