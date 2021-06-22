using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Indexing.TimeSeries
{
    public class BasicTimeSeriesIndexes_MixedSyntax : RavenTestBase
    {
        public BasicTimeSeriesIndexes_MixedSyntax(ITestOutputHelper output)
            : base(output)
        {
        }

        [Fact]
        public void BasicMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = RavenTestHelper.UtcToday;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 7 }, "tag");

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var result = store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeSeries.Companies.HeartRate.Where(x => true) " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Date = entry.Timestamp.Date, " +
                    "   User = ts.DocumentId " +
                    "}" }
                }));

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now2, new double[] { 3 }, "tag");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation("MyTsIndex")).EntriesCount, 2));

                var terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Date", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal(now1.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);


                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/1", terms);

                // delete time series

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Delete(now2);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("7", terms);

                // delete document

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");
                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(0, terms.Length);


                // delete document - this time don't stop indexing to make sure doc deletion will be noticed by the index

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/2");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 9 }, "tag");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("9", terms);


                using (var session = store.OpenSession())
                {
                    session.Delete("companies/2");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(0, terms.Length);
            }
        }

    }
}
