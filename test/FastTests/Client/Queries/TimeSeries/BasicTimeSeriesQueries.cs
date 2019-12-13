using System;
using System.Linq;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
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

        private class TsIndexResult
        {
            public long HeartBeat { get; set; }
            public DateTime Date { get; set; }
            public string User { get; set; }
        }

        [Fact]
        public void BasicMapIndex_Query()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company).Append("HeartRate", now1, "tag", new double[] { 7 });

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
                    "   Date = entry.TimeStamp.Date, " +
                    "   User = ts.DocumentId.ToString() " +
                    "}" }
                }));

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(0, results.Count);
                }

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(1, results.Count);
                    Assert.Equal(7, results[0].HeartBeat);
                    Assert.Equal(now1.Date, results[0].Date);
                    Assert.Equal("companies/1", results[0].User);
                }

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company).Append("HeartRate", now2, "tag", new double[] { 3 });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(1, results.Count);
                    Assert.Equal(7, results[0].HeartBeat);
                    Assert.Equal(now1.Date, results[0].Date);
                    Assert.Equal("companies/1", results[0].User);
                }

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsIndexResult>("MyTsIndex")
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
                    session.TimeSeriesFor(company).Remove("HeartRate", now2);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsIndexResult>("MyTsIndex")
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

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsIndexResult>("MyTsIndex")
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
                    var results = session.Query<TsIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(1, results.Count);
                    Assert.Contains(7, results.Select(x => x.HeartBeat));
                    Assert.Contains(now1.Date, results.Select(x => x.Date));
                }

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsIndexResult>("MyTsIndex")
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
                    session.TimeSeriesFor(company).Append("HeartRate", now1, "tag", new double[] { 9 });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<TsIndexResult>("MyTsIndex")
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
                    var results = session.Query<TsIndexResult>("MyTsIndex")
                        .Statistics(out var stats)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(0, results.Count);
                }
            }
        }
    }
}
