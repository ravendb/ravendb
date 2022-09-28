using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18768 : RavenTestBase
{
    public RavenDB_18768(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Revisions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public async Task CanIncludeRevisionsByPathInLoadByIdQuery(Options options)
    {
        const string id = "users/rhino";

        using (var store = GetDocumentStore(options))
        {
            await RevisionsHelper.SetupRevisionsAsync(store);

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Arek", }, id);

                session.SaveChanges();
            }

            string changeVector;
            using (var session = store.OpenSession())
            {
                var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                Assert.Equal(1, metadatas.Count);

                changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query = session.Query<User>().Where(x => x.Id == id)
                    .Include(builder => builder.IncludeRevisions(x => x.FirstRevision));

                query.Customize(c => c.WaitForNonStaleResults()).ToList();

                var revision1 = session.Advanced.Revisions.Get<User>(changeVector);
                Assert.NotNull(revision1);
                Assert.Null(revision1.FirstRevision);

                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Revisions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public async Task CanIncludeRevisionsBeforeInLoadByIdQuery(Options options)
    {
        const string id = "users/rhino";

        using (var store = GetDocumentStore(options))
        {
            await RevisionsHelper.SetupRevisionsAsync(store);

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Arek", }, id);

                session.SaveChanges();
            }

            string changeVector;
            using (var session = store.OpenSession())
            {
                var metadatas = session.Advanced.Revisions.GetMetadataFor(id);
                Assert.Equal(1, metadatas.Count);

                changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
            }

            var beforeDateTime = DateTime.UtcNow;
            using (var session = store.OpenSession())
            {
                var query = session.Query<User>().Where(x => x.Id == id)
                    .Include(builder => builder
                        .IncludeRevisions(beforeDateTime));
                var users = query.Customize(x => x.WaitForNonStaleResults()).ToList();

                var revision = session.Advanced.Revisions.Get<User>(changeVector);

                Assert.NotNull(users);
                Assert.Equal(users.Count, 1);
                Assert.NotNull(revision);
                Assert.NotNull(revision.Name);
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }
    }

    [RavenTheory(RavenTestCategory.TimeSeries)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void CanIncludeMultipleTimeSeriesInLoadByIdQuery(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var baseline = RavenTestHelper.UtcToday;

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Oren" }, "users/ayende");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 360; i++)
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddSeconds(i * 10), new[] { 6d }, "watches/fitbit");
                    session.TimeSeriesFor("users/ayende", "BloodPressure")
                        .Append(baseline.AddSeconds(i * 10), new[] { 66d }, "watches/fitbit");
                    session.TimeSeriesFor("users/ayende", "Nasdaq")
                        .Append(baseline.AddSeconds(i * 10), new[] { 8097.23 }, "nasdaq.com");
                }

                session.SaveChanges();

            }

            using (var session = store.OpenSession())
            {
                var user = session.Query<User>()
                    .Where(x => x.Id == "users/ayende")
                    .Include(i => i.IncludeTimeSeries("Heartrate", baseline.AddMinutes(3), baseline.AddMinutes(5))
                        .IncludeTimeSeries("BloodPressure", baseline.AddMinutes(40), baseline.AddMinutes(45))
                        .IncludeTimeSeries("Nasdaq", baseline.AddMinutes(15), baseline.AddMinutes(25)))
                    .First();

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                Assert.Equal("Oren", user.Name);

                // should not go to server

                var vals = session.TimeSeriesFor("users/ayende", "Heartrate")
                    .Get(baseline.AddMinutes(3), baseline.AddMinutes(5))
                    .ToList();

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                Assert.Equal(13, vals.Count);
                Assert.Equal(baseline.AddMinutes(3), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(5), vals[12].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                // should not go to server

                vals = session.TimeSeriesFor("users/ayende", "BloodPressure")
                    .Get(baseline.AddMinutes(42), baseline.AddMinutes(43))
                    .ToList();

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                Assert.Equal(7, vals.Count);
                Assert.Equal(baseline.AddMinutes(42), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(43), vals[6].Timestamp, RavenTestHelper.DateTimeComparer.Instance);


                // should not go to server

                vals = session.TimeSeriesFor("users/ayende", "BloodPressure")
                    .Get(baseline.AddMinutes(40), baseline.AddMinutes(45))
                    .ToList();

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                Assert.Equal(31, vals.Count);
                Assert.Equal(baseline.AddMinutes(40), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(45), vals[30].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

                // should not go to server

                vals = session.TimeSeriesFor("users/ayende", "Nasdaq")
                    .Get(baseline.AddMinutes(15), baseline.AddMinutes(25))
                    .ToList();

                Assert.Equal(1, session.Advanced.NumberOfRequests);

                Assert.Equal(61, vals.Count);
                Assert.Equal(baseline.AddMinutes(15), vals[0].Timestamp, RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(baseline.AddMinutes(25), vals[60].Timestamp, RavenTestHelper.DateTimeComparer.Instance);

            }
        }
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string ChangeVector { get; set; }
        public string FirstRevision { get; set; }
        public string SecondRevision { get; set; }
        public string ThirdRevision { get; set; }
        public List<string> ChangeVectors { get; set; }
    }
}
