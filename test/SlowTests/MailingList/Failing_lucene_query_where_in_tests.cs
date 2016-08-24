using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Xunit;

namespace SlowTests.MailingList
{
    public class Failing_lucene_query_where_in_tests : RavenTestBase
    {
        private class Contract
        {
            public string Cno { get; set; }
            public IEnumerable<ContractDetail> ContractDetails { get; set; }
            public string Id { get; set; }
        }

        private class ContractDetail
        {
            public int DetailsId { get; set; }
            public string Grade { get; set; }
        }

        private class ContractDto
        {
            public string Cno { get; set; }
            public int DetailsId { get; set; }
            public string Grade { get; set; }
            public string Id { get; set; }
        }

        private class Contract_ToContractListViewModel : AbstractIndexCreationTask<Contract, ContractDto>
        {
            public Contract_ToContractListViewModel()
            {
                Map = contracts => from c in contracts
                                   from d in c.ContractDetails
                                   select new ContractDto
                                   {
                                       Id = c.Id,
                                       Cno = c.Cno,
                                       DetailsId = d.DetailsId,
                                       Grade = d.Grade
                                   };

                Store(x => x.Id, FieldStorage.Yes);
                Store(x => x.Cno, FieldStorage.Yes);
                Store(x => x.DetailsId, FieldStorage.Yes);
                Store(x => x.Grade, FieldStorage.Yes);
            }
        }

        private Contract GetContract()
        {
            return new Contract
            {
                Id = "contracts-1",
                Cno = "ABC123",
                ContractDetails = new List<ContractDetail>
                {
                    new ContractDetail {DetailsId = 1, Grade = "Grade1"},
                    new ContractDetail {DetailsId = 2, Grade = "Grade1"}
                }
            };
        }

        protected override void ModifyStore(DocumentStore store)
        {
            store.RegisterListener(new NonStaleQueryListener());
        }

        [Fact]
        public void Failing_query_using_embedded_store_but_works_against_real_database()
        {
            // Arrange
            var contract = GetContract();

            using (var store = GetDocumentStore())
            {
                new Contract_ToContractListViewModel().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(contract);
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var contractDetails = new[] { 1, 2 };

                    var query = session.Advanced.DocumentQuery<Contract, Contract_ToContractListViewModel>()
                            .Projection<Contract, ContractDto>()
                            .WhereIn(c => c.DetailsId, contractDetails);

                    // returns --> "@in<DetailsId>:(1,2)"

                    var results = query.ToList();

                    // Assert
                    // this fails using the NewDocumentStore, but works against the real database
                    Assert.True(results.Count > 0);
                }
            }
        }

        [Fact]
        public void Failing_lucene_query_where_in()
        {
            // Arrange
            var contract = GetContract();

            using (var store = GetDocumentStore())
            {
                new Contract_ToContractListViewModel().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(contract);
                    session.SaveChanges();
                }


                // Act
                using (var session = store.OpenSession())
                {
                    var contractA = session.Load<Contract>("contracts-1");
                    Assert.Equal(2, contractA.ContractDetails.Count());

                    var contractDetails = new int[] { };

                    var query =
                        session.Advanced.DocumentQuery<Contract, Contract_ToContractListViewModel>()
                            .Projection<Contract, ContractDto>()
                            .WhereIn(c => c.DetailsId, contractDetails);

                    var results = query.ToList();

                    // Assert
                    Assert.Equal(0, results.Count);
                }
            }
        }



        [Fact]
        public void Failing_query_using_embedded_store1()
        {
            // Arrange
            var contract = GetContract();

            using (var store = GetDocumentStore())
            {
                new Contract_ToContractListViewModel().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(contract);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var contractA = session.Load<Contract>("contracts-1");
                    Assert.Equal(2, contractA.ContractDetails.Count());

                    // Act
                    var queryA = session.Query<Contract, Contract_ToContractListViewModel>().ToList();

                    // Assert
                    // This works against the real database, but not using the NewDocumentStore
                    Assert.Equal(1, queryA.Count());

                }
            }
        }

        [Fact]
        public void Failing_query_using_embedded_store2()
        {
            // Arrange
            var contract = GetContract();

            using (var store = GetDocumentStore())
            {
                new Contract_ToContractListViewModel().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(contract);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var contractA = session.Load<Contract>("contracts-1");
                    Assert.Equal(2, contractA.ContractDetails.Count());

                    // Act
                    var queryB = session.Advanced.DocumentQuery<Contract, Contract_ToContractListViewModel>()
                        .Projection<Contract, ContractDto>()
                        .ToList();

                    // Assert
                    // This works against the real database, but not using the NewDocumentStore
                    Assert.Equal(2, queryB.Count);
                }
            }
        }


        private class NonStaleQueryListener : IDocumentQueryListener
        {
            public void BeforeQueryExecuted(IDocumentQueryCustomization customization)
            {
                customization.WaitForNonStaleResults();
            }
        }
    }

    public static class Extensions
    {
        public static IDocumentQuery<TS> Projection<T, TS>(this IDocumentQuery<T> query)
        {
            return query.SelectFields<TS>(typeof(TS).GetTypeInfo().GetProperties().Select(x => x.Name).ToArray());
        }
    }
}
