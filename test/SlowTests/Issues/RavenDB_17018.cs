using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17018 : RavenTestBase
    {
        public RavenDB_17018(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldStoreTotalDocumentSizeInPerformanceHint_ForLoadDocuments()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.PerformanceHints.MaxNumberOfResults)] = "1"
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR", Address = new Address { Country = "Japan" } }, "companies/1");
                    session.Store(new Company { Name = "CF", Address = new Address { Country = "Russia" } }, "companies/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var companies = session.Load<Company>(new[] { "companies/1", "companies/2" }).ToList();
                    Assert.NotEmpty(companies);
                }

                var database = await GetDatabase(store.Database);

                var performanceHints = WaitForValue(() =>
                {
                    database.NotificationCenter.Paging.UpdatePagingInternal(null, out string _);
                    return database.NotificationCenter.GetPerformanceHintCount();
                }, 1);

                Assert.Equal(1, performanceHints);

                using (database.NotificationCenter.GetStored(out var actions))
                {
                    var action = actions.First();
                    Assert.True(action.Json.TryGet("Details", out BlittableJsonReaderObject details));
                    Assert.True(details.TryGet("Actions", out BlittableJsonReaderObject detailsActions));
                    Assert.True(detailsActions.TryGet("GetDocumentsByIdAsync", out BlittableJsonReaderArray detailsArray));
                    var index = detailsArray.GetByIndex<BlittableJsonReaderObject>(0);
                    Assert.NotNull(index);
                    Assert.True(index.TryGet("TotalDocumentsSizeInBytes", out int size));
                    Assert.True(size > 0);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldStoreTotalDocumentSizeInPerformanceHint_ForQueries(Options options)
        {
            options.ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.PerformanceHints.MaxNumberOfResults)] = "1";
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR", Address = new Address { Country = "Japan" } }, "companies/1");
                    session.Store(new Company { Name = "CF", Address = new Address { Country = "Russia" } }, "companies/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var companies = session.Query<Company>().ToList();
                    Assert.NotEmpty(companies);
                }

                var database = await GetDatabase(store.Database);

                var performanceHints = WaitForValue(() =>
                {
                    database.NotificationCenter.Paging.UpdatePagingInternal(null, out string _);
                    return database.NotificationCenter.GetPerformanceHintCount();
                }, 1);

                Assert.Equal(1, performanceHints);

                using (database.NotificationCenter.GetStored(out var actions))
                {
                    var action = actions.First();
                    Assert.True(action.Json.TryGet("Details", out BlittableJsonReaderObject details));
                    Assert.True(details.TryGet("Actions", out BlittableJsonReaderObject detailsActions));
                    Assert.True(detailsActions.TryGet("Query (collection/Companies)", out BlittableJsonReaderArray detailsArray));
                    var index = detailsArray.GetByIndex<BlittableJsonReaderObject>(0);
                    Assert.NotNull(index);
                    Assert.True(index.TryGet("TotalDocumentsSizeInBytes", out int size));
                    Assert.True(size > 0);
                }
            }
        }

        [Fact]
        public async Task ShouldStoreTotalDocumentSizeInPerformanceHint_ForQueriesWithIncludes()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.PerformanceHints.MaxNumberOfResults)] = "1"
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new B { SomeString = "b1" }, "b/1");
                    session.Store(new B { SomeString = "b2" }, "b/2");
                    session.Store(new A { BId = "b/1" }, "a/1");
                    session.Store(new A { BId = "b/2" }, "a/2");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var aObjects = session
                        .Query<A>()
                        .Include(x => x.BId)
                        .ToList();

                    Assert.NotEmpty(aObjects);
                }

                var database = await GetDatabase(store.Database);

                var performanceHints = WaitForValue(() =>
                {
                    database.NotificationCenter.Paging.UpdatePagingInternal(null, out string _);
                    return database.NotificationCenter.GetPerformanceHintCount();
                }, 1);

                Assert.Equal(1, performanceHints);

                using (database.NotificationCenter.GetStored(out var actions))
                {
                    var action = actions.First();
                    Assert.True(action.Json.TryGet("Details", out BlittableJsonReaderObject details));
                    Assert.True(details.TryGet("Actions", out BlittableJsonReaderObject detailsActions));
                    Assert.True(detailsActions.TryGet("Query (collection/As)", out BlittableJsonReaderArray detailsArray));
                    var index = detailsArray.GetByIndex<BlittableJsonReaderObject>(0);
                    Assert.NotNull(index);
                    Assert.True(index.TryGet("TotalDocumentsSizeInBytes", out int size));
                    Assert.True(size > 0);
                }
            }
        }

        [Fact]
        public async Task ShouldStoreTotalDocumentsSizeInPerformanceHint_ForRevisions()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.PerformanceHints.MaxNumberOfResults)] = "1"
            }))
            {
                var database = await GetDatabase(store.Database);
                database.NotificationCenter.Paging.ForTestingPurposesOnly().DisableTimer = true;

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR", Address = new Address { Country = "Japan" } }, "companies/1");

                    var myRevisionsConfiguration = new RevisionsConfiguration
                    {
                        Default = new RevisionsCollectionConfiguration
                        {
                            Disabled = false,
                            PurgeOnDelete = false,
                            MinimumRevisionsToKeep = 5,
                            MinimumRevisionAgeToKeep = TimeSpan.FromDays(14),
                        },

                        Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                        {
                            {"companies", new RevisionsCollectionConfiguration {}}
                        }
                    };
                    var revisionsConfigurationOperation = new ConfigureRevisionsOperation(myRevisionsConfiguration);
                    store.Maintenance.Send(revisionsConfigurationOperation);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var comp = session.Load<Company>("companies/1");
                    Assert.NotNull(comp);
                    comp.Phone = "123456789";
                    comp.Fax = "0987654321";
                    session.SaveChanges();

                    comp.Fax = "123456789";
                    comp.Phone = "0987654321";
                    session.SaveChanges();

                    var revisions = session
                        .Advanced
                        .Revisions
                        .GetFor<Company>("companies/1", pageSize: 4);
                }

                var outcome = database.NotificationCenter.Paging.UpdatePagingInternal(null, out string reason);
                Assert.True(outcome, reason);


                using (database.NotificationCenter.GetStored(out var actions))
                {
                    var action = actions.First();
                    Assert.True(action.Json.TryGet("Details", out BlittableJsonReaderObject details));
                    Assert.True(details.TryGet("Actions", out BlittableJsonReaderObject detailsActions));
                    Assert.True(detailsActions.TryGet("GetRevisions", out BlittableJsonReaderArray detailsArray));
                    var index = detailsArray.GetByIndex<BlittableJsonReaderObject>(0);
                    Assert.NotNull(index);
                    Assert.True(index.TryGet("TotalDocumentsSizeInBytes", out int size));
                    Assert.True(size > 0);
                }
            }
        }

        [Fact]
        public async Task ShouldStoreTotalDocumentSizeInPerformanceHint_ForCompareExchange()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.PerformanceHints.MaxNumberOfResults)] = "1"
            }))
            {
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/1", new Company { Name = "HR" });
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/2", new Company { Name = "CF" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var results = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Company>(new[] { "companies/1", "companies/2" });
                    Assert.Equal(2, results.Count);
                    Assert.NotNull(results["companies/1"].Value);
                    Assert.NotNull(results["companies/2"].Value);
                }

                var database = await GetDatabase(store.Database);

                var performanceHints = WaitForValue(() =>
                {
                    database.NotificationCenter.Paging.UpdatePagingInternal(null, out string _);
                    return database.NotificationCenter.GetPerformanceHintCount();
                }, 1);

                Assert.Equal(1, performanceHints);

                using (database.NotificationCenter.GetStored(out var actions))
                {
                    var action = actions.First();
                    Assert.True(action.Json.TryGet("Details", out BlittableJsonReaderObject details));
                    Assert.True(details.TryGet("Actions", out BlittableJsonReaderObject detailsAction));
                    Assert.True(detailsAction.TryGet("GetCompareExchangeValuesByKey", out BlittableJsonReaderArray cmpxchvalues));
                    var index = cmpxchvalues.GetByIndex<BlittableJsonReaderObject>(0);
                    Assert.NotNull(index);
                    Assert.True(index.TryGet("TotalDocumentsSizeInBytes", out int size));
                    Assert.True(size > 0);
                }
            }
        }

        [Fact]
        public void NullCompareExchangeValuesDoNotBreakTotalDocumentsSizeAccumulating()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var address = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    Assert.Null(address);

                    var addresses = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Address>(new[] { "companies/hr", "companies/cf" });

                    Assert.True(addresses.ContainsKey("companies/hr"));
                    Assert.True(addresses.ContainsKey("companies/cf"));
                    Assert.Null(addresses["companies/hr"]);
                    Assert.Null(addresses["companies/cf"]);
                }
            }
        }

        private class A
        {
            public string BId { get; set; }
        }

        private class B
        {
            public string SomeString { get; set; }
        }
    }
}

