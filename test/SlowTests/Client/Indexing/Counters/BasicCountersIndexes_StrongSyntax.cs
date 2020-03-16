using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Indexing.Counters
{
    public class BasicCountersIndexes_StrongSyntax : RavenTestBase
    {
        public BasicCountersIndexes_StrongSyntax(ITestOutputHelper output) : base(output)
        {
        }

        private class MyCounterIndex : AbstractCountersIndexCreationTask<Company>
        {
            public MyCounterIndex()
            {
                AddMap("HeartRate", counters => from counter in counters
                                                select new
                                                {
                                                    HeartBeat = counter.Value,
                                                    Name = counter.Name,
                                                    User = counter.DocumentId
                                                });
            }
        }

        [Fact]
        public void BasicMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.CountersFor(company).Increment("HeartRate", 7);

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyCounterIndex();
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();
                RavenTestHelper.AssertEqualRespectingNewLines("counters.Companies.HeartRate.Select(counter => new {\r\n    HeartBeat = counter.Value,\r\n    Name = counter.Name,\r\n    User = counter.DocumentId\r\n})", indexDefinition.Maps.First());

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                var terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("7", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                using (var session = store.OpenSession())
                {
                    var company1 = session.Load<Company>("companies/1");
                    session.CountersFor(company1).Increment("HeartRate", 3);

                    var company2 = new Company();
                    session.Store(company2, "companies/2");
                    session.CountersFor(company2).Increment("HeartRate", 4);

                    var company3 = new Company();
                    session.Store(company3, "companies/3");
                    session.CountersFor(company3).Increment("HeartRate", 6);

                    var company999 = new Company();
                    session.Store(company999, "companies/999");
                    session.CountersFor(company999).Increment("HeartRate_Different", 999);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("10", terms);
                Assert.Contains("4", terms);
                Assert.Contains("6", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "User", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("companies/1", terms);
                Assert.Contains("companies/2", terms);
                Assert.Contains("companies/3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                // delete counter

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.CountersFor(company).Delete("HeartRate");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("4", terms);
                Assert.Contains("6", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "User", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("companies/2", terms);
                Assert.Contains("companies/3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                // delete document

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/2");
                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("6", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                // delete document - live

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");
                    session.Delete("companies/3");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(0, terms.Length);

                // add document with counter - live

                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company, "companies/4");
                    session.CountersFor(company).Increment("HeartRate", 5);

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("5", terms);
            }
        }
    }
}
