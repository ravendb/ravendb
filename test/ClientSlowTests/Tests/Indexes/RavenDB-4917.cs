using System.Linq;
using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Xunit;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Operations.Databases;
using Raven.NewClient.Operations.Databases.Indexes;

namespace NewClientTests.NewClient
{
    public class RavenDB_4917 : RavenNewTestBase
    {
        [Fact(Skip = "RavenDB-5919")]
        public void side_by_side_doesnt_create_new_index()
        {
            using (var store = GetDocumentStore())
            {
                var databaseStatistics = store.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(0, databaseStatistics.CountOfIndexes);

                store.Admin.Send(new StopIndexingOperation());
                
                store.ExecuteIndex(new Customer_Index());

                store.SideBySideExecuteIndex(new Customer_Index());

                databaseStatistics = store.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(1, databaseStatistics.CountOfIndexes);

                store.ExecuteIndex(new Customer_Index());

                store.SideBySideExecuteIndex(new Customer_Index());

                databaseStatistics = store.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(2, databaseStatistics.CountOfIndexes);

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

                Index(e => e.Id, FieldIndexing.Analyzed);
                Index(e => e.FirstName, FieldIndexing.Default);
                Index(e => e.LastName, FieldIndexing.Default);
                Index(e => e.DisplayName, FieldIndexing.Default);

                Analyze(e => e.FirstName, "StandardAnalyzer");

                Sort(p => p.LastName, SortOptions.String);
                Sort(p => p.FirstName, SortOptions.String);
                Sort(p => p.DisplayName, SortOptions.String);
            }
        }

        private class Customer_Index2 : AbstractIndexCreationTask<Customer, Customer_Index.IndexEntry>
        {
            public class IndexEntry
            {
                public string Id { get; set; }
                public string FirstName { get; set; }
                public string LastName { get; set; }
                public string DisplayName { get; set; }
            }

            public Customer_Index2()
            {
                Map = customers => from customer in customers
                                   select new IndexEntry
                                   {
                                       Id = customer.Id,
                                       LastName = customer.LastName,
                                       FirstName = customer.FirstName,
                                       DisplayName = customer.DisplayName
                                   };

                Index(e => e.Id, FieldIndexing.Analyzed);
                Index(e => e.FirstName, FieldIndexing.Default);
                Index(e => e.LastName, FieldIndexing.Default);
                Index(e => e.DisplayName, FieldIndexing.Default);

                Analyze(e => e.FirstName, "StandardAnalyzer");

                Sort(p => p.LastName, SortOptions.String);
                Sort(p => p.FirstName, SortOptions.String);
                Sort(p => p.DisplayName, SortOptions.String);
            }
        }
    }
}
