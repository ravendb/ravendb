using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4917 : RavenTestBase
    {
        public RavenDB_4917(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void side_by_side_doesnt_create_new_index()
        {
            using (var store = GetDocumentStore())
            {
                Assert.Equal(0, store.Maintenance.Send(new GetStatisticsOperation()).CountOfIndexes);

                store.Maintenance.Send(new StopIndexingOperation());

                store.ExecuteIndex(new Customer_Index());
                store.ExecuteIndex(new Customer_Index()); // potential side-by-side

                Assert.Equal(1, store.Maintenance.Send(new GetStatisticsOperation()).CountOfIndexes);
            }
        }

        private class Customer
        {
            public string Id { get; set; }

            public string FirstName { get; set; }

            public string LastName { get; set; }

            public string DisplayName { get; set; }
        }

        private class Customer_Index : AbstractIndexCreationTask<Customer, Customer_Index.IndexEntry>
        {
            public class IndexEntry
            {
                public string Id { get; set; }
                public string FirstName { get; set; }
                public string LastName { get; set; }
                public string DisplayName { get; set; }
            }

            public Customer_Index()
            {
                Map = customers => from customer in customers
                                   select new IndexEntry
                                   {
                                       Id = customer.Id,
                                       LastName = customer.LastName,
                                       FirstName = customer.FirstName,
                                       DisplayName = customer.DisplayName
                                   };

                Index(e => e.Id, FieldIndexing.Search);
                Index(e => e.FirstName, FieldIndexing.Default);
                Index(e => e.LastName, FieldIndexing.Default);
                Index(e => e.DisplayName, FieldIndexing.Default);

                Analyze(e => e.FirstName, "StandardAnalyzer");
            }
        }
    }
}
