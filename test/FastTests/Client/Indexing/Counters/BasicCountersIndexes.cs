using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Indexing.Counters
{
    public class BasicCountersIndexes : RavenTestBase
    {
        public BasicCountersIndexes(ITestOutputHelper output) : base(output)
        {
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

                string indexName = "MyCounterIndex";

                var result = store.Maintenance.Send(new PutIndexesOperation(new CountersIndexDefinition
                {
                    Name = indexName,
                    Maps = {
                    "from counter in counters.Companies.HeartRate " +
                    "let company = LoadDocument(counter.DocumentId, \"Companies\")" +
                    "let employee = LoadDocument(company.Desc, \"Employees\")" +
                    "select new { " +
                    "   HeartBeat = counter.Value, " +
                    "   User = counter.DocumentId, " +
                    "   Employee = employee.FirstName" +
                    "}" }
                }));

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

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

                WaitForIndexing(store);

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

                WaitForIndexing(store);

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

                WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex(indexName);

                using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/11");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

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

                string indexName = "AverageHeartRate";

                var result = store.Maintenance.Send(new PutIndexesOperation(new CountersIndexDefinition
                {
                    Name = indexName,
                    Maps = {
                    "from counter in counters.Users.HeartRate " +
                    "select new { " +
                    "   HeartBeat = counter.Value, " +
                    "   Name = counter.Name, " +
                    "   Count = 1" +
                    "}" },
                    Reduce = "from r in results " +
                             "group r by r.Name into g " +
                             "let sumHeartBeat = g.Sum(x => x.HeartBeat) " +
                             "let sumCount = g.Sum(x => x.Count) " +
                             "select new {" +
                             "  HeartBeat = sumHeartBeat / sumCount, " +
                             "  Name = g.Key, " +
                             "  Count = sumCount" +
                             "}"
                }));

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

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

                WaitForIndexing(store);

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

                WaitForIndexing(store);

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

                WaitForIndexing(store);

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

                string indexName = "AverageHeartRate";

                var result = store.Maintenance.Send(new PutIndexesOperation(new CountersIndexDefinition
                {
                    Name = indexName,
                    Maps = {
                    "from counter in counters.Users.HeartRate " +
                    "let user = LoadDocument(counter.DocumentId, \"Users\") " +
                    "let address = LoadDocument(user.AddressId, \"Addresses\") " +
                    "select new { " +
                    "   HeartBeat = counter.Value, " +
                    "   City = address.City, " +
                    "   Count = 1" +
                    "}" },
                    Reduce = "from r in results " +
                             "group r by r.City into g " +
                             "let sumHeartBeat = g.Sum(x => x.HeartBeat) " +
                             "let sumCount = g.Sum(x => x.Count) " +
                             "select new {" +
                             "  HeartBeat = sumHeartBeat / sumCount, " +
                             "  City = g.Key, " +
                             "  Count = sumCount" +
                             "}"
                }));

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

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

                WaitForIndexing(store);

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

                WaitForIndexing(store);

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

                WaitForIndexing(store);

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
    }
}
