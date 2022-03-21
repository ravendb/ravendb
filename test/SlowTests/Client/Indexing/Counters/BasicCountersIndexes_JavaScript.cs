using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Indexing.Counters
{
    public class BasicCountersIndexes_JavaScript : RavenTestBase
    {
        public BasicCountersIndexes_JavaScript(ITestOutputHelper output)
            : base(output)
        {
        }

        private class MyCounterIndex : AbstractJavaScriptCountersIndexCreationTask
        {
            public MyCounterIndex()
            {
                Maps = new HashSet<string>
                {
                    @"counters.map('Companies', 'HeartRate', function (counter) {
return {
    HeartBeat: counter.Value,
    Name: counter.Name,
    User: counter.DocumentId
};
})"
                };
            }
        }

        private class MyCounterIndex_Load : AbstractJavaScriptCountersIndexCreationTask
        {
            public MyCounterIndex_Load()
            {
                Maps = new HashSet<string>
                {
                    @"counters.map('Companies', 'HeartRate', function (counter) {
var company = load(counter.DocumentId, 'Companies');
var employee = load(company.Desc, 'Employees');
return {
    HeartBeat: counter.Value,
    User: counter.DocumentId,
    Employee: employee.FirstName
};
})"
                };
            }
        }

        private class AverageHeartRate : AbstractJavaScriptCountersIndexCreationTask
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public string Name { get; set; }

                public long Count { get; set; }
            }

            public AverageHeartRate()
            {
                Maps = new HashSet<string>
                {
                    @"counters.map('Users', 'HeartRate', function (counter) {
return {
    HeartBeat: counter.Value,
    Count: 1,
    Name: counter.Name
};
})"
                };

                Reduce = @"groupBy(r => ({ Name: r.Name }))
                             .aggregate(g => ({
                                 HeartBeat: g.values.reduce((total, val) => val.HeartBeat + total, 0) / g.values.reduce((total, val) => val.Count + total, 0),
                                 Name: g.key.Name,
                                 Count: g.values.reduce((total, val) => val.Count + total, 0)
                             }))";
            }
        }

        private class AverageHeartRate_WithLoad : AbstractJavaScriptCountersIndexCreationTask
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public string City { get; set; }

                public long Count { get; set; }
            }

            public AverageHeartRate_WithLoad()
            {
                Maps = new HashSet<string>
                {
                    @"counters.map('Users', 'HeartRate', function (counter) {
var user = load(counter.DocumentId, 'Users');
var address = load(user.AddressId, 'Addresses');
return {
    HeartBeat: counter.Value,
    Count: 1,
    City: address.City
};
})"
                };

                Reduce = @"groupBy(r => ({ City: r.City }))
                             .aggregate(g => ({
                                 HeartBeat: g.values.reduce((total, val) => val.HeartBeat + total, 0) / g.values.reduce((total, val) => val.Count + total, 0),
                                 City: g.key.City,
                                 Count: g.values.reduce((total, val) => val.Count + total, 0)
                             }))";
            }
        }

        private class MyCounterIndex_AllCounters : AbstractJavaScriptCountersIndexCreationTask
        {
            public MyCounterIndex_AllCounters()
            {
                Maps = new HashSet<string>
                {
                    @"counters.map('Companies', function (counter) {
return {
    HeartBeat: counter.Value,
    Name: counter.Name,
    User: counter.DocumentId
};
})"
                };
            }
        }

        private class MyCounterIndex_AllDocs : AbstractJavaScriptCountersIndexCreationTask
        {
            public MyCounterIndex_AllDocs()
            {
                Maps = new HashSet<string>
                {
                    @"counters.map(function (counter) {
return {
    HeartBeat: counter.Value,
    Name: counter.Name,
    User: counter.DocumentId
};
})"
                };
            }
        }

        private class MyMultiMapCounterIndex : AbstractJavaScriptCountersIndexCreationTask
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public string Name { get; set; }

                public string User { get; set; }
            }

            public MyMultiMapCounterIndex()
            {
                Maps = new HashSet<string>
                {
                    @"counters.map('Companies', 'HeartRate', function (counter) {
return {
    HeartBeat: counter.Value,
    Name: counter.Name,
    User: counter.DocumentId
};
})",
                    @"counters.map('Companies', 'HeartRate2', function (counter) {
return {
    HeartBeat: counter.Value,
    Name: counter.Name,
    User: counter.DocumentId
};
})",
                    @"counters.map('Users', 'HeartRate', function (counter) {
return {
    HeartBeat: counter.Value,
    Name: counter.Name,
    User: counter.DocumentId
};
})"
                };
            }
        }

        private class Companies_ByCounterNames : AbstractJavaScriptIndexCreationTask
        {
            public Companies_ByCounterNames()
            {
                Maps = new HashSet<string>
                {
                    @"map('Companies', function (company) {
return ({
    Names: counterNamesFor(company)
})
})"
                };
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

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyCounterIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyCounterIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("5", terms);
            }
        }

        [Fact]
        public async Task BasicMapIndexWithLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var employee = new Employee
                    {
                        FirstName = "John"
                    };
                    session.Store(employee, "employees/1");

                    var company = new Company
                    {
                        Desc = employee.Id
                    };
                    session.Store(company, "companies/1");

                    session.CountersFor(company).Increment("HeartRate", 7);

                    var company2 = new Company
                    {
                        Desc = employee.Id
                    };
                    session.Store(company2, "companies/11");

                    session.CountersFor(company2).Increment("HeartRate", 11);

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyCounterIndex_Load();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Employee", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("john", terms);

                using (var session = store.OpenSession())
                {
                    var employee = session.Load<Employee>("employees/1");
                    employee.FirstName = "Bob";

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Employee", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("bob", terms);

                using (var session = store.OpenSession())
                {
                    session.Delete("employees/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Employee", null));
                Assert.Equal(0, terms.Length);

                // delete source document

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(2, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.All(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex(indexName);

                using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(2, counts.CollectionTableCount); // companies/11, employees/1
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/11");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);
                }
            }
        }

        [Fact]
        public void BasicMapReduceIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var user = new User();
                        session.Store(user, $"users/{i}");

                        session.CountersFor(user).Increment("HeartRate", 180 + i);
                    }

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new AverageHeartRate();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("184.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("10", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                // add more heart rates
                using (var session = store.OpenSession())
                {
                    for (int i = 10; i < 20; i++)
                    {
                        var user = new User();
                        session.Store(user, $"users/{i}");

                        session.CountersFor(user).Increment("HeartRate", 200 + i);
                    }

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("199.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("20", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                //// delete some counters

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var user = session.Load<User>($"users/{i}");

                        session.CountersFor(user).Delete("HeartRate");
                    }

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("214.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("10", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                //// delete documents

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 20; i++)
                        session.Delete($"users/{i}");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [Fact]
        public async Task BasicMapReduceIndexWithLoad()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var address = new Address { City = "NY" };
                        session.Store(address, $"addresses/{i}");

                        var user = new User { AddressId = address.Id };
                        session.Store(user, $"users/{i}");

                        session.CountersFor(user).Increment("HeartRate", 180 + i);
                    }

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new AverageHeartRate_WithLoad();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("184.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("10", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("ny", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        var address = session.Load<Address>($"addresses/{i}");
                        address.City = "LA";
                    }

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("ny", terms);
                Assert.Contains("la", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    for (int i = 5; i < 10; i++)
                        session.Delete($"addresses/{i}");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("la", terms);
                Assert.Contains("NULL_VALUE", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                        session.Delete($"users/{i}");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(2, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.All(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(0, terms.Length);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex(indexName);

                using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);
                }
            }
        }

        [Fact]
        public void CanMapAllCountersFromCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.CountersFor(company).Increment("HeartRate", 7);
                    session.CountersFor(company).Increment("Likes", 3);

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyCounterIndex_AllCounters();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/2");
                    session.CountersFor(company).Increment("HeartRate", 13);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("13", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("companies/1", terms);
                Assert.Contains("companies/2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = new User();
                    session.Store(company, "users/1");
                    session.CountersFor(company).Increment("HeartRate", 13);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("13", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/2");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [Fact]
        public void CanMapAllCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.CountersFor(company).Increment("HeartRate", 7);
                    session.CountersFor(company).Increment("Likes", 3);

                    var employee = new Employee();
                    session.Store(employee, "employees/1");
                    session.CountersFor(employee).Increment("Dislikes", 1);

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyCounterIndex_AllDocs();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("companies/1", terms);
                Assert.Contains("employees/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);
                Assert.Contains("dislikes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.CountersFor(company).Increment("HeartRate", 2);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("9", terms);
                Assert.Contains("3", terms);
                Assert.Contains("1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("companies/1", terms);
                Assert.Contains("employees/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Name", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);
                Assert.Contains("dislikes", terms);

                store.Maintenance.Send(new StopIndexingOperation());
            }
        }

        [Fact]
        public async Task BasicMultiMapIndex()
        {
            using (var store = GetDocumentStore())
            {
                var timeSeriesIndex = new MyMultiMapCounterIndex();
                await timeSeriesIndex.ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company);

                    session.CountersFor(company).Increment("HeartRate", 3);
                    session.CountersFor(company).Increment("HeartRate2", 5);

                    var user = new User();
                    session.Store(user);
                    session.CountersFor(user).Increment("HeartRate", 2);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<MyMultiMapCounterIndex.Result, MyMultiMapCounterIndex>()
                        .ToList();

                    Assert.Equal(3, results.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var results = await session.Query<MyMultiMapCounterIndex.Result, MyMultiMapCounterIndex>()
                        .ToListAsync();

                    Assert.Equal(3, results.Count);
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<MyMultiMapCounterIndex.Result, MyMultiMapCounterIndex>()
                        .ToList();

                    Assert.Equal(3, results.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var results = await session.Advanced.AsyncDocumentQuery<MyMultiMapCounterIndex.Result, MyMultiMapCounterIndex>()
                        .ToListAsync();

                    Assert.Equal(3, results.Count);
                }
            }
        }

        [Fact]
        public void CounterNamesFor()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Companies_ByCounterNames();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Names", null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Names_IsArray", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("true", terms);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    session.CountersFor(company).Increment("HeartRate", 3);
                    session.CountersFor(company).Increment("HeartRate2", 7);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Names", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("heartrate2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Names_IsArray", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("true", terms);
            }
        }
    }
}
