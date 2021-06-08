using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Indexing.TimeSeries
{
    public class BasicTimeSeriesIndexes : RavenTestBase
    {
        public BasicTimeSeriesIndexes(ITestOutputHelper output) : base(output)
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
                    "from ts in timeseries.Companies.HeartRate " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Name = ts.Name," +
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

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("heartrate", terms);

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

        [Fact]
        public async Task BasicMapIndexWithLoad()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var employee = new Employee
                    {
                        FirstName = "John"
                    };
                    session.Store(employee, "employees/1");

                    var company = new Company();
                    session.Store(company, "companies/1");

                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 7 }, employee.Id);

                    var company2 = new Company();
                    session.Store(company2, "companies/11");

                    session.TimeSeriesFor(company2, "HeartRate").Append(now1, new double[] { 11 }, employee.Id);

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                string indexName = "MyTsIndex";

                var result = store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = indexName,
                    Maps = {
                    "from ts in timeSeries.Companies.HeartRate " +
                    "from entry in ts.Entries " +
                    "let employee = LoadDocument(entry.Tag, \"Employees\")" +
                    "select new { " +
                    "   HeartBeat = entry.Value, " +
                    "   Date = entry.Timestamp.Date, " +
                    "   User = ts.DocumentId, " +
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

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 2));

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

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 2));

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

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 2));

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
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

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

                Assert.Equal(0, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 0));

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
                var today = RavenTestHelper.UtcToday;
                var tomorrow = today.AddDays(1);

                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user, "users/1");

                    var tsf = session.TimeSeriesFor(user, "HeartRate");
                    
                    for (int i = 0; i < 10; i++)
                    {
                        tsf.Append(today.AddHours(i), new double[] { 180 + i }, "abc");
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
                    "   User = ts.DocumentId, " +
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

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Date", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal(today.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("users/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("10", terms[0]);

                store.Maintenance.Send(new StopIndexingOperation());

                // add more heart rates
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    var tsf = session.TimeSeriesFor(user, "HeartRate");

                    for (int i = 0; i < 20; i++)
                    {
                        tsf.Append(tomorrow.AddHours(i), new double[] { 200 + i }, "abc");
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

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 2));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("184.5", terms);
                Assert.Contains("209.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Date", null));
                Assert.Equal(2, terms.Length);
                Assert.Equal(today.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(tomorrow.Date, DateTime.Parse(terms[1]), RavenTestHelper.DateTimeComparer.Instance);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("users/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("10", terms);
                Assert.Contains("20", terms);


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

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Date", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal(tomorrow.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);


                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("users/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("10", terms[0]);

                //// delete document

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
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

                WaitForUserToContinueTheTest(store);
            }
        }

        [Fact]
        public async Task BasicMapReduceIndexWithLoad()
        {
            {
                using (var store = GetDocumentStore())
                {
                    var today = RavenTestHelper.UtcToday;

                    using (var session = store.OpenSession())
                    {
                        var address = new Address { City = "NY" };

                        session.Store(address, "addresses/1");

                        var user = new User();

                        user.AddressId = address.Id;

                        session.Store(user, "users/1");

                        var tsf = session.TimeSeriesFor(user, "HeartRate");

                        for (int i = 0; i < 10; i++)
                        {
                            tsf.Append(today.AddHours(i), 180 + i, tag: address.Id);
                        }

                        session.SaveChanges();
                    }

                    store.Maintenance.Send(new StopIndexingOperation());

                    string indexName = "AverageHeartRateDaily/ByDateAndCity";

                    var result = store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                    {
                        Name = indexName,
                        Maps =
                        {
                            "from ts in timeSeries.Users.HeartRate " +
                            "from entry in ts.Entries " +
                            "let address = LoadDocument(entry.Tag, \"Addresses\")" +
                            "select new { " +
                            "   HeartBeat = entry.Value, " +
                            "   Date = new DateTime(entry.Timestamp.Date.Year, entry.Timestamp.Date.Month, entry.Timestamp.Date.Day), " +
                            "   City = address.City, " +
                            "   Count = 1" +
                            "}"
                        },
                        Reduce = "from r in results " +
                                 "group r by new { r.Date, r.City } into g " +
                                 "let sumHeartBeat = g.Sum(x => x.HeartBeat) " +
                                 "let sumCount = g.Sum(x => x.Count) " +
                                 "select new {" +
                                 "  HeartBeat = sumHeartBeat / sumCount, " +
                                 "  Date = g.Key.Date," +
                                 "  City = g.Key.City, " +
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

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Date", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Equal(today.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("ny", terms);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Equal("10", terms[0]);

                    store.Maintenance.Send(new StopIndexingOperation());

                    using (var session = store.OpenSession())
                    {
                        var address = session.Load<Address>("addresses/1");
                        address.City = "LA";

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
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("la", terms);

                    store.Maintenance.Send(new StopIndexingOperation());

                    using (var session = store.OpenSession())
                    {
                        session.Delete("addresses/1");

                        session.SaveChanges();
                    }

                    staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                    store.Maintenance.Send(new StartIndexingOperation());

                    WaitForIndexing(store);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Equal("NULL_VALUE", terms[0]);

                    // delete source document

                    store.Maintenance.Send(new StopIndexingOperation());

                    using (var session = store.OpenSession())
                    {
                        session.Delete("users/1");

                        session.SaveChanges();
                    }

                    staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                    store.Maintenance.Send(new StartIndexingOperation());

                    WaitForIndexing(store);

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

        [Fact]
        public async Task TimeSeriesForDocumentIdWithEscapePositions()
        {
            var str = "Oren\r\nEini";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var entity = new User { Name = str };
                    await session.StoreAsync(entity, str);

                    session.TimeSeriesFor(entity, "HeartRate").Append(DateTime.Now, new double[] { 7 }, "tag");

                    await session.SaveChangesAsync();
                }

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
                        "   User = ts.DocumentId, " +
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

                WaitForIndexing(store);

                WaitForUserToContinueTheTest(store);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains(str.ToLower(), terms);

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(str);
                    var id = session.Advanced.GetDocumentId(u);

                    Assert.Equal(str, u.Name);
                    Assert.Equal(str, id);
                }
            }
        }

        [Fact]
        public void MapIndexWithCaseInsensitiveTimeSeriesNames()
        {
            using (var store = GetDocumentStore())
            {
                var now = DateTime.Now;

                string heartRateTimeSeriesName = "HeartRate";

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, heartRateTimeSeriesName).Append(now, new double[] { 13 }, "tag");

                    session.SaveChanges();
                }

                var result = store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeSeries.Companies.HeartRate " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Date = entry.Timestamp.Date, " +
                    "   User = ts.DocumentId " +
                    "}" }
                }));

                WaitForIndexing(store);

                var terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("13", terms);

                // delete time series

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "hearTraTe").Delete(now); // <--- note casing hearTraTe

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [Fact]
        public void CanUpdateMapTimeSeriesIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeSeries.Companies.HeartRate " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Date = entry.Timestamp.Date, " +
                    "   User = ts.DocumentId " +
                    "}" }
                }));


                store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                        "from ts in timeSeries.Companies.ChangedTs " +
                        "from entry in ts.Entries " +
                        "select new { " +
                        "   HeartBeat = entry.Value, " +
                        "   Date = entry.Timestamp.Date, " +
                        "   User = ts.DocumentId " +
                        "}" }
                }));

                WaitForIndexing(store);

                Assert.True(SpinWait.SpinUntil(() => store.Maintenance.Send(new GetIndexesOperation(0, 10)).Length == 1, TimeSpan.FromSeconds(20)));

                var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));

                Assert.Contains("ChangedTs", indexes[0].Maps.First());
            }
        }

        [Fact]
        public void CanUpdateMapTimeSeriesIndexWithoutUpdatingCompiledIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                        "from ts in timeSeries.Companies.HeartRate " +
                        "from entry in ts.Entries " +
                        "select new { " +
                        "   HeartBeat = entry.Values[0], " +
                        "   Date = entry.Timestamp.Date, " +
                        "   User = ts.DocumentId " +
                        "}" },
                    Priority = IndexPriority.Low
                }));

                store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                        "from ts in timeSeries.Companies.HeartRate " +
                        "from entry in ts.Entries " +
                        "select new { " +
                        "   HeartBeat = entry.Values[0], " +
                        "   Date = entry.Timestamp.Date, " +
                        "   User = ts.DocumentId " +
                        "}" },
                    Priority = IndexPriority.High
                }));

                WaitForIndexing(store);

                Assert.True(SpinWait.SpinUntil(() => store.Maintenance.Send(new GetIndexesOperation(0, 10)).Length == 1, TimeSpan.FromSeconds(20)));

                var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));

                Assert.Equal(IndexPriority.High, indexes[0].Priority);
            }
        }

        [Fact]
        public void CanUpdateMapReduceTimeSeriesIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MPTSIndex",
                    Maps = {
                        "from ts in timeSeries.Users.HeartRate " +
                        "from entry in ts.Entries " +
                        "select new { " +
                        "   HeartBeat = entry.Value, " +
                        "   Date = new DateTime(entry.Timestamp.Date.Year, entry.Timestamp.Date.Month, entry.Timestamp.Date.Day), " +
                        "   User = ts.DocumentId, " +
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


                store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MPTSIndex",
                    Maps = {
                        "from ts in timeSeries.Users.HeartRate " +
                        "from entry in ts.Entries " +
                        "select new { " +
                        "   HeartBeat = entry.Value, " +
                        "   Date = new DateTime(entry.Timestamp.Date.Year, entry.Timestamp.Date.Month, entry.Timestamp.Date.Day), " +
                        "   User = ts.DocumentId, " +
                        "   Count = 1" +
                        "}" },
                    Reduce = "from r in results " +
                             "group r by new { r.Date, r.User } into g " +
                             "let sumHeartBeat = g.Sum(x => x.HeartBeat) " +
                             "let sumCount = g.Sum(x => x.Count) " +
                             "select new {" +
                             "  HeartBeat = sumCount / sumHeartBeat, " + // <--- changed
                             "  Date = g.Key.Date," +
                             "  User = g.Key.User, " +
                             "  Count = sumCount" +
                             "}"
                }));

                WaitForIndexing(store);

                Assert.True(SpinWait.SpinUntil(() => store.Maintenance.Send(new GetIndexesOperation(0, 10)).Length == 1, TimeSpan.FromSeconds(20)));

                var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));

                Assert.Contains("sumCount / sumHeartBeat", indexes[0].Reduce);
            }
        }

        [Fact]
        public void CanUpdateMapReduceTimeSeriesIndexWithoutUpdatingCompiledIndex()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MPTSIndex",
                    Maps = {
                        "from ts in timeSeries.Users.HeartRate " +
                        "from entry in ts.Entries " +
                        "select new { " +
                        "   HeartBeat = entry.Value, " +
                        "   Date = new DateTime(entry.Timestamp.Date.Year, entry.Timestamp.Date.Month, entry.Timestamp.Date.Day), " +
                        "   User = ts.DocumentId, " +
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
                             "}",
                    Priority = IndexPriority.Low
                }));

                store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MPTSIndex",
                    Maps = {
                        "from ts in timeSeries.Users.HeartRate " +
                        "from entry in ts.Entries " +
                        "select new { " +
                        "   HeartBeat = entry.Value, " +
                        "   Date = new DateTime(entry.Timestamp.Date.Year, entry.Timestamp.Date.Month, entry.Timestamp.Date.Day), " +
                        "   User = ts.DocumentId, " +
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
                             "}",
                    Priority = IndexPriority.High
                }));

                WaitForIndexing(store);

                Assert.True(SpinWait.SpinUntil(() => store.Maintenance.Send(new GetIndexesOperation(0, 10)).Length == 1, TimeSpan.FromSeconds(20)));

                var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10));

                Assert.Equal(IndexPriority.High, indexes[0].Priority);
            }
        }

        [Fact]
        public void CanMapAllTimeSeriesFromCollection()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 7 }, "tag1");
                    session.TimeSeriesFor(company, "Likes").Append(now1, new double[] { 3 }, "tag2");

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var result = store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeSeries.Companies " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Name = ts.Name, " +
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

                var terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now2, new double[] { 2 }, "tag");

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
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Name", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Delete( now1);

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
                Assert.Equal(2, terms.Length);
                Assert.Contains("3", terms);
                Assert.Contains("2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Name", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

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
                Assert.Contains("3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("likes", terms);

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

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Name", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [Fact]
        public void CanMapAllTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 7 }, "tag1");
                    session.TimeSeriesFor(company, "Likes").Append(now1, new double[] { 3 }, "tag2");

                    var employee = new Employee();
                    session.Store(employee, "employees/1");
                    session.TimeSeriesFor(employee, "Dislikes").Append(now1, new double[] { 1 }, "tag3");

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var result = store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeSeries " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Name = ts.Name, " +
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

                var terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Name", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);
                Assert.Contains("dislikes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now2, new double[] { 2 }, "tag");

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
                Assert.Equal(4, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("1", terms);
                Assert.Contains("2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Name", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);
                Assert.Contains("dislikes", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "Likes").Delete(now1, now2);

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
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("1", terms);
                Assert.Contains("2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Name", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("dislikes", terms);

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
                Assert.Equal(1, terms.Length);
                Assert.Contains("1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("dislikes", terms);

                // now checking live updates (not stopping indexing)

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("employees/1");
                    session.TimeSeriesFor(company, "Dislikes").Append(now2, 9, "tag");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("1", terms);
                Assert.Contains("9", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("dislikes", terms);

                using (var session = store.OpenSession())
                {
                    session.Delete("employees/1");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Name", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [Fact]
        public void SupportForEscapedCollectionAndTimeSeriesNames()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindCollectionName = t =>
                {
                    if (t == typeof(Company))
                        return "Companies With Space";

                    return null;
                }
            }))
            {
                var indexDefinition = new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeSeries[@ \"Companies With Space\"][@ \"Heart Rate\"] " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Name = ts.Name, " +
                    "   Date = entry.Timestamp.Date, " +
                    "   User = ts.DocumentId " +
                    "}" }
                };

                AssertIndex(store, indexDefinition, "Heart Rate");
            }

            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindCollectionName = t =>
                {
                    if (t == typeof(Company))
                        return "Companies With Space";

                    return null;
                }
            }))
            {
                var indexDefinition = new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeSeries[@ \"Companies With Space\"].HeartRate " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Name = ts.Name, " +
                    "   Date = entry.Timestamp.Date, " +
                    "   User = ts.DocumentId " +
                    "}" }
                };

                AssertIndex(store, indexDefinition, "HeartRate");

                indexDefinition = new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeSeries[@ \"Companies With Space\"] " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Name = ts.Name, " +
                    "   Date = entry.Timestamp.Date, " +
                    "   User = ts.DocumentId " +
                    "}" }
                };

                AssertIndex(store, indexDefinition, "HeartRate");
            }

            using (var store = GetDocumentStore())
            {
                var indexDefinition = new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeSeries.Companies[@ \"Heart Rate\"] " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Name = ts.Name, " +
                    "   Date = entry.Timestamp.Date, " +
                    "   User = ts.DocumentId " +
                    "}" }
                };

                AssertIndex(store, indexDefinition, "Heart Rate");
            }

            static void AssertIndex(IDocumentStore store, IndexDefinition definition, string timeSeriesName)
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, timeSeriesName).Append(now1, new double[] { 7 }, "tag");

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                store.Maintenance.Send(new PutIndexesOperation(definition));

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(definition.Name));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(definition.Name));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, timeSeriesName).Append(now2, new double[] { 3 }, "tag");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(definition.Name));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(definition.Name));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(definition.Name));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(definition.Name));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new DeleteIndexOperation(definition.Name));
            }
        }

        [Fact]
        public async Task CanPersist()
        {
            using (var store = GetDocumentStore(new Options
            {
                RunInMemory = false
            }))
            {
                var dbName = store.Database;

                var indexDefinition1 = new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeseries.Companies.HeartRate " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Name = ts.Name," +
                    "   Date = entry.Timestamp.Date, " +
                    "   User = ts.DocumentId " +
                    "}" }
                };

                var indexDefinition2 = new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex2",
                    Maps = {
                    "from ts in timeSeries.Users.HeartRate " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Value, " +
                    "   Date = new DateTime(entry.Timestamp.Date.Year, entry.Timestamp.Date.Month, entry.Timestamp.Date.Day), " +
                    "   User = ts.DocumentId, " +
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
                };

                await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition1, indexDefinition2));

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(dbName);

                var indexDefinitions = await store.Maintenance.SendAsync(new GetIndexesOperation(0, 25));
                Assert.Equal(2, indexDefinitions.Length);

                indexDefinitions = indexDefinitions
                    .OrderBy(x => x.Name.Length)
                    .ToArray();

                var index = indexDefinitions[0];

                Assert.Equal(IndexType.Map, index.Type);
                Assert.Equal(IndexSourceType.TimeSeries, index.SourceType);
                Assert.Equal("MyTsIndex", index.Name);
                Assert.Equal(IndexLockMode.Unlock, index.LockMode);
                Assert.Equal(IndexPriority.Normal, index.Priority);
                Assert.True(indexDefinition1.Equals(index));

                index = indexDefinitions[1];

                Assert.Equal(IndexType.MapReduce, index.Type);
                Assert.Equal(IndexSourceType.TimeSeries, index.SourceType);
                Assert.Equal("MyTsIndex2", index.Name);
                Assert.Equal(IndexLockMode.Unlock, index.LockMode);
                Assert.Equal(IndexPriority.Normal, index.Priority);
                Assert.True(indexDefinition2.Equals(index));

                var database = await GetDatabase(dbName);

                var indexes = database
                    .IndexStore
                    .GetIndexes()
                    .OrderBy(x => x.Name.Length)
                    .ToList();

                Assert.Equal(IndexType.Map, indexes[0].Type);
                Assert.Equal(IndexSourceType.TimeSeries, indexes[0].SourceType);
                Assert.Equal("MyTsIndex", indexes[0].Name);
                Assert.Equal(1, indexes[0].Definition.Collections.Count);
                Assert.Equal("Companies", indexes[0].Definition.Collections.Single());
                Assert.Equal(4, indexes[0].Definition.MapFields.Count);
                Assert.Contains("Name", indexes[0].Definition.MapFields.Keys);
                Assert.Contains("HeartBeat", indexes[0].Definition.MapFields.Keys);
                Assert.Contains("Date", indexes[0].Definition.MapFields.Keys);
                Assert.Contains("User", indexes[0].Definition.MapFields.Keys);
                Assert.Equal(IndexLockMode.Unlock, indexes[0].Definition.LockMode);
                Assert.Equal(IndexPriority.Normal, indexes[0].Definition.Priority);
                Assert.True(indexDefinition1.Equals(indexes[0].GetIndexDefinition()));

                Assert.Equal(IndexType.MapReduce, indexes[1].Type);
                Assert.Equal(IndexSourceType.TimeSeries, indexes[1].SourceType);
                Assert.Equal("MyTsIndex2", indexes[1].Name);
                Assert.Equal(1, indexes[1].Definition.Collections.Count);
                Assert.Equal("Users", indexes[1].Definition.Collections.Single());
                Assert.Equal(4, indexes[1].Definition.MapFields.Count);
                Assert.Contains("HeartBeat", indexes[1].Definition.MapFields.Keys);
                Assert.Contains("Date", indexes[1].Definition.MapFields.Keys);
                Assert.Contains("User", indexes[1].Definition.MapFields.Keys);
                Assert.Contains("Count", indexes[1].Definition.MapFields.Keys);
                Assert.Equal(IndexLockMode.Unlock, indexes[1].Definition.LockMode);
                Assert.Equal(IndexPriority.Normal, indexes[1].Definition.Priority);
                Assert.True(indexDefinition2.Equals(indexes[1].GetIndexDefinition()));
            }
        }
    }
}
