using System;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Extensions;
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
        public void BasicIndex()
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

                var result = store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeSeries.Companies.HeartRate " +
                    "from entry in ts.Entries " +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Date = entry.TimeStamp.Date, " +
                    "   User = ts.DocumentId " +
                    "}" }
                }));

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company).Append("HeartRate", now2, "tag", new double[] { 3 });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation("MyTsIndex")).EntriesCount, 2));

                var terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Date", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains(now1.Date.GetDefaultRavenFormat(), terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/1", terms);
            }
        }

        [Fact]
        public void BasicIndexWithLoad()
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

                    session.TimeSeriesFor(company).Append("HeartRate", now1, "employees/1", new double[] { 7 });

                    session.SaveChanges();
                }

                var result = store.Maintenance.Send(new PutIndexesOperation(new TimeSeriesIndexDefinition
                {
                    Name = "MyTsIndex",
                    Maps = {
                    "from ts in timeSeries.Companies.HeartRate " +
                    "from entry in ts.Entries " +
                    "let employee = LoadDocument(entry.Tag, \"Employees\")" +
                    "select new { " +
                    "   HeartBeat = entry.Values[0], " +
                    "   Date = entry.TimeStamp.Date, " +
                    "   User = ts.DocumentId, " +
                    "   Employee = employee.FirstName" +
                    "}" }
                }));

                WaitForIndexing(store);

                Assert.Equal(1, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation("MyTsIndex")).EntriesCount, 1));

                var terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Employee", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("john", terms);

                using (var session = store.OpenSession())
                {
                    var employee = session.Load<Employee>("employees/1");
                    employee.FirstName = "Bob";

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                Assert.Equal(1, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation("MyTsIndex")).EntriesCount, 1));

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Employee", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("bob", terms);
            }
        }
    }
}
