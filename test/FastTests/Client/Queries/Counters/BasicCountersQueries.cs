using System.Linq;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Queries.Counters
{
    public class BasicCountersQueries : RavenTestBase
    {
        public BasicCountersQueries(ITestOutputHelper output) : base(output)
        {
        }

        private class CounterMapIndexResult
        {
            public double HeartBeat { get; set; }
            public string Name { get; set; }
            public string User { get; set; }
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

                var result = store.Maintenance.Send(new PutIndexesOperation(new CountersIndexDefinition
                {
                    Name = "MyCounterIndex",
                    Maps = {
                    "from counter in counters.Companies.HeartRate " +
                    "select new { " +
                    "   HeartBeat = counter.Value, " +
                    "   Name = counter.Name," +
                    "   User = counter.DocumentId " +
                    "}" }
                }));

                using (var session = store.OpenSession())
                {
                    var results = session.Query<CounterMapIndexResult>("MyCounterIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(0, results.Count);
                }

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = (DocumentSession)store.OpenSession())
                {
                    var results = session.Query<CounterMapIndexResult>("MyCounterIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(1, results.Count);
                    Assert.Equal(7, results[0].HeartBeat);
                    Assert.Equal("HeartRate", results[0].Name);
                    Assert.Equal("companies/1", results[0].User);

                    // check if we are tracking the results
                    Assert.Equal(0, session.DocumentsById.Count);
                    Assert.Equal(0, session.DocumentsByEntity.Count);
                }

                store.Maintenance.Send(new StopIndexingOperation());

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

                using (var session = (DocumentSession)store.OpenSession())
                {
                    var results = session.Query<CounterMapIndexResult>("MyCounterIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(1, results.Count);
                    Assert.Equal(7, results[0].HeartBeat);
                    Assert.Equal("HeartRate", results[0].Name);
                    Assert.Equal("companies/1", results[0].User);
                }

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = (DocumentSession)store.OpenSession())
                {
                    var results = session.Query<CounterMapIndexResult>("MyCounterIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(3, results.Count);

                    Assert.Contains(10, results.Select(x => x.HeartBeat));
                    Assert.Contains("companies/1", results.Select(x => x.User));

                    Assert.Contains(4, results.Select(x => x.HeartBeat));
                    Assert.Contains("companies/2", results.Select(x => x.User));

                    Assert.Contains(6, results.Select(x => x.HeartBeat));
                    Assert.Contains("companies/3", results.Select(x => x.User));

                    Assert.True(results.All(x => x.Name == "HeartRate"));
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // delete counter

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.CountersFor(company).Delete("HeartRate");

                    session.SaveChanges();
                }

                using (var session = (DocumentSession)store.OpenSession())
                {
                    var results = session.Query<CounterMapIndexResult>("MyCounterIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(3, results.Count);

                    Assert.Contains(10, results.Select(x => x.HeartBeat));
                    Assert.Contains("companies/1", results.Select(x => x.User));

                    Assert.Contains(4, results.Select(x => x.HeartBeat));
                    Assert.Contains("companies/2", results.Select(x => x.User));

                    Assert.Contains(6, results.Select(x => x.HeartBeat));
                    Assert.Contains("companies/3", results.Select(x => x.User));

                    Assert.True(results.All(x => x.Name == "HeartRate"));
                }

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = (DocumentSession)store.OpenSession())
                {
                    var results = session.Query<CounterMapIndexResult>("MyCounterIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(2, results.Count);

                    Assert.Contains(4, results.Select(x => x.HeartBeat));
                    Assert.Contains("companies/2", results.Select(x => x.User));

                    Assert.Contains(6, results.Select(x => x.HeartBeat));
                    Assert.Contains("companies/3", results.Select(x => x.User));

                    Assert.True(results.All(x => x.Name == "HeartRate"));
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // delete document

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/2");
                    session.SaveChanges();
                }

                using (var session = (DocumentSession)store.OpenSession())
                {
                    var results = session.Query<CounterMapIndexResult>("MyCounterIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.True(stats.IsStale);
                    Assert.Equal(2, results.Count);

                    Assert.Contains(4, results.Select(x => x.HeartBeat));
                    Assert.Contains("companies/2", results.Select(x => x.User));

                    Assert.Contains(6, results.Select(x => x.HeartBeat));
                    Assert.Contains("companies/3", results.Select(x => x.User));

                    Assert.True(results.All(x => x.Name == "HeartRate"));
                }

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = (DocumentSession)store.OpenSession())
                {
                    var results = session.Query<CounterMapIndexResult>("MyCounterIndex")
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(1, results.Count);

                    Assert.Contains(6, results.Select(x => x.HeartBeat));
                    Assert.Contains("companies/3", results.Select(x => x.User));

                    Assert.True(results.All(x => x.Name == "HeartRate"));
                }

                // delete document - live

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");
                    session.Delete("companies/3");
                    session.SaveChanges();
                }

                using (var session = (DocumentSession)store.OpenSession())
                {
                    var results = session.Query<CounterMapIndexResult>("MyCounterIndex")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(0, results.Count);
                }

                // add document with counter - live

                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR" };
                    session.Store(company, "companies/4");
                    session.CountersFor(company).Increment("HeartRate", 5);

                    session.SaveChanges();
                }

                using (var session = (DocumentSession)store.OpenSession())
                {
                    var results = session.Query<CounterMapIndexResult>("MyCounterIndex")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Statistics(out var stats)
                        .ToList();

                    Assert.False(stats.IsStale);
                    Assert.Equal(1, results.Count);

                    Assert.Contains(5, results.Select(x => x.HeartBeat));
                    Assert.Contains("companies/4", results.Select(x => x.User));

                    Assert.True(results.All(x => x.Name == "HeartRate"));
                }
            }
        }

    }
}
