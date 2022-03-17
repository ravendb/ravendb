using System;
using System.Linq;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Queries.TimeSeries
{
    public class BasicTimeSeriesQueries : RavenTestBase
    {
        public BasicTimeSeriesQueries(ITestOutputHelper output)
            : base(output)
        {
        }

        private class TsMapIndexResult
        {
            public double HeartBeat { get; set; }
            public DateTime Date { get; set; }
            public string User { get; set; }
        }

        private class TsMapReduceIndexResult : TsMapIndexResult
        {
            public long Count { get; set; }
        }

        [Fact]
        public void BasicMapIndex_Query()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = RavenTestHelper.UtcToday;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, 7, "tag");

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var result = store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeSeries.Companies.HeartRate " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Date = entry.Timestamp.Date, " +
                    "   User = ts.DocumentId.ToString() " +
                    "}" }
                }));

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(0, results.Count);
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = (DocumentSession)store.OpenSession())
                {
                    var results = session.Query<TsMapIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(1, results.Count);
                    Assert.Equal(7, results[0].HeartBeat);
                    Assert.Equal(now1.Date, results[0].Date, RavenTestHelper.DateTimeComparer.Instance);
                    Assert.Equal("companies/1", results[0].User);

                    // check if we are tracking the results
                    Assert.Equal(0, session.DocumentsById.Count);
                    Assert.Equal(0, session.DocumentsByEntity.Count);
                }

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now2, new double[] { 3 }, "tag");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(1, results.Count);
                    Assert.Equal(7, results[0].HeartBeat);
                    Assert.Equal(now1.Date, results[0].Date);
                    Assert.Equal("companies/1", results[0].User);
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(2, results.Count);
                    Assert.Contains(7, results.Select(x => x.HeartBeat));
                    Assert.Contains(now1.Date, results.Select(x => x.Date));
                    Assert.Contains(3, results.Select(x => x.HeartBeat));
                    Assert.Contains(now2.Date, results.Select(x => x.Date));
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // delete time series

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Delete(now2);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(2, results.Count);
                    Assert.Contains(7, results.Select(x => x.HeartBeat));
                    Assert.Contains(now1.Date, results.Select(x => x.Date));
                    Assert.Contains(3, results.Select(x => x.HeartBeat));
                    Assert.Contains(now2.Date, results.Select(x => x.Date));
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(1, results.Count);
                    Assert.Contains(7, results.Select(x => x.HeartBeat));
                    Assert.Contains(now1.Date, results.Select(x => x.Date));
                }

                // delete document

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(1, results.Count);
                    Assert.Contains(7, results.Select(x => x.HeartBeat));
                    Assert.Contains(now1.Date, results.Select(x => x.Date));
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(0, results.Count);
                }

                // delete document - this time don't stop indexing to make sure doc deletion will be noticed by the index

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/2");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 9 }, "tag");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(1, results.Count);
                    Assert.Contains(9, results.Select(x => x.HeartBeat));
                    Assert.Contains(now1.Date, results.Select(x => x.Date));
                    Assert.Contains("companies/2", results.Select(x => x.User));
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(0, results.Count);
                }
            }
        }

        [Fact]
        public void BasicMapReduceIndex_Query()
        {
            using (var store = GetDocumentStore())
            {
                var today = RavenTestHelper.UtcToday;
                var tomorrow = today.AddDays(1);

                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user, "users/1");

                    for (int i = 0; i < 10; i++)
                    {
                        session.TimeSeriesFor(user, "HeartRate").Append(today.AddHours(i), new double[] { 180 + i }, "abc");
                    }

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                string indexName = "AverageHeartRateDaily/ByDateAndUser";

                var result = store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = indexName,
                    Maps = {
                    "from ts in timeSeries.Users.HeartRate " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Value, " +
                    "   Date = new DateTime(entry.Timestamp.Date.Year, entry.Timestamp.Date.Month, entry.Timestamp.Date.Day), " +
                    "   User = ts.DocumentId.ToString(), " + // TODO arek RavenDB-14322
                    "   Count = 1" +
                    "}" },
                    Reduce = "from r in results " +
                             "group r by new { r.Date, r.User } into g " +
                             "let sumHeartBeat = g.Sum(x => x.HeartBeat) " +
                             "let sumCount = g.Sum(x => x.Count) " +
                             "select new {" +
                             "  HeartBeat = sumHeartBeat / sumCount, " +
                             "  Date = g.Key.Date," +
                             "  User = g.Key.User, " +
                             "  Count = sumCount" +
                             "}"
                }));

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapReduceIndexResult>(indexName)
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(0, results.Count);
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = (DocumentSession)store.OpenSession())
                {
                    var results = session.Query<TsMapReduceIndexResult>(indexName)
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(1, results.Count);
                    Assert.Contains(184.5, results.Select(x => x.HeartBeat));
                    Assert.Contains(today.Date, results.Select(x => x.Date));
                    Assert.Contains("users/1", results.Select(x => x.User));
                    Assert.Contains(10, results.Select(x => x.Count));

                    // check if we are tracking the results
                    Assert.Equal(0, session.DocumentsById.Count);
                    Assert.Equal(0, session.DocumentsByEntity.Count);
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // add more heart rates
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    for (int i = 0; i < 20; i++)
                    {
                        session.TimeSeriesFor(user, "HeartRate").Append(tomorrow.AddHours(i), new double[] { 200 + i }, "abc");
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapReduceIndexResult>(indexName)
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(1, results.Count);
                    Assert.Contains(184.5, results.Select(x => x.HeartBeat));
                    Assert.Contains(today.Date, results.Select(x => x.Date));
                    Assert.Contains("users/1", results.Select(x => x.User));
                    Assert.Contains(10, results.Select(x => x.Count));
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapReduceIndexResult>(indexName)
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(2, results.Count);

                    Assert.Contains(184.5, results.Select(x => x.HeartBeat));
                    Assert.Contains(today.Date, results.Select(x => x.Date));
                    Assert.Contains(10, results.Select(x => x.Count));

                    Assert.Contains(209.5, results.Select(x => x.HeartBeat));
                    Assert.Contains(tomorrow.Date, results.Select(x => x.Date));
                    Assert.Contains(20, results.Select(x => x.Count));

                    Assert.Equal(2, results.Select(x => x.User == "users/1").Count());
                }

                store.Maintenance.Send(new StopIndexingOperation());

                //// delete some time series

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var tsf = session.TimeSeriesFor(user, "HeartRate");

                    for (int i = 0; i < 10; i++)
                    {
                        tsf.Delete(today.AddHours(i));
                        tsf.Delete(tomorrow.AddHours(i));
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapReduceIndexResult>(indexName)
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(2, results.Count);

                    Assert.Contains(184.5, results.Select(x => x.HeartBeat));
                    Assert.Contains(today.Date, results.Select(x => x.Date));
                    Assert.Contains(10, results.Select(x => x.Count));

                    Assert.Contains(209.5, results.Select(x => x.HeartBeat));
                    Assert.Contains(tomorrow.Date, results.Select(x => x.Date));
                    Assert.Contains(20, results.Select(x => x.Count));

                    Assert.Equal(2, results.Select(x => x.User == "users/1").Count());
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapReduceIndexResult>(indexName)
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(1, results.Count);

                    Assert.Contains(214.5, results.Select(x => x.HeartBeat));
                    Assert.Contains(tomorrow.Date, results.Select(x => x.Date));
                    Assert.Contains(10, results.Select(x => x.Count));

                    Assert.Equal(1, results.Select(x => x.User == "users/1").Count());
                }

                //// delete document

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapReduceIndexResult>(indexName)
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(1, results.Count);

                    Assert.Contains(214.5, results.Select(x => x.HeartBeat));
                    Assert.Contains(tomorrow.Date, results.Select(x => x.Date));
                    Assert.Contains(10, results.Select(x => x.Count));

                    Assert.Equal(1, results.Select(x => x.User == "users/1").Count());
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsMapReduceIndexResult>(indexName)
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(0, results.Count);
                }
            }
        }

    }
}
