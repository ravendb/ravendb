using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Xunit;
using FastTests;

namespace SlowTests.MailingList.Rusty
{
    public class ProjectionCachingTests : RavenTestBase
    {
        [Fact]
        public void Projection_Fails_To_Update_Property_After_Associated_Document_Updates()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex( new PersonIndex() );

                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "ABC" };
                    session.Store( company );

                    var person = new Person { FirstName = "Bill", LastName = "Smith", CompanyReference = new DocumentReference { Id = company.Id } };
                    session.Store( person );

                    session.SaveChanges();
                }

                WaitForIndexing( store );

                using (var session = store.OpenSession())
                {
                    var result = QueryPersonIndex( session );
                    Assert.Equal( "ABC", result );
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Query<Company>().Single();
                    company.Name = "XYZ";
                    session.SaveChanges();
                }

                WaitForIndexing( store );

                using (var session = store.OpenSession())
                {
                    var result = QueryPersonIndex( session );
                    Assert.Equal( "XYZ", result );
                }
            }
        }

        [Fact]
        public void Projection_Updates_Property_When_After_Document_Updates_If_Index_Loads_Association()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex( new PersonWithCompanyIndex() );

                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "ABC" };
                    session.Store( company );

                    var person = new Person { FirstName = "Bill", LastName = "Smith", CompanyReference = new DocumentReference { Id = company.Id } };
                    session.Store( person );

                    session.SaveChanges();
                }

                WaitForIndexing( store );

                using (var session = store.OpenSession())
                {
                    var result = QueryPersonWithCompanyIndex( session );
                    Assert.Equal( "ABC", result );
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Query<Company>().Single();
                    company.Name = "XYZ";
                    session.SaveChanges();
                }

                WaitForIndexing( store );

                using (var session = store.OpenSession())
                {
                    var result = QueryPersonWithCompanyIndex( session );
                    WaitForUserToContinueTheTest(store);
                    Assert.Equal( "XYZ", result );
                }
            }
        }

        private static string QueryPersonIndex( IDocumentSession session )
        {
            var queryable = from p in session
                    .Query<PersonIndex.Result, PersonIndex>()
                    .Where( x => x.LastName == "Smith" )
                    .OfType<Person>()
                let company = RavenQuery.Load<Company>( p.CompanyReference.Id )
                select new PersonCompanyProjection
                {
                    PersonId = p.Id,
                    CompanyName = company.Name
                };

            var result = queryable.Single().CompanyName;
            return result;
        }

        private static string QueryPersonWithCompanyIndex( IDocumentSession session )
        {
            var queryable = from p in session
                    .Query<PersonWithCompanyIndex.Result, PersonWithCompanyIndex>()
                    .Where( x => x.LastName == "Smith" )
                    .OfType<Person>()
                let company = RavenQuery.Load<Company>( p.CompanyReference.Id )
                select new PersonCompanyProjection
                {
                    PersonId = p.Id,
                    CompanyName = company.Name
                };

            var result = queryable.Single().CompanyName;
            return result;
        }

        private class Person
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public DocumentReference CompanyReference { get; set; }
        }

        private class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class DocumentReference
        {
            public string Id { get; set; }
        }

        private class PersonIndex : AbstractIndexCreationTask<Person, PersonIndex.Result>
        {
            public PersonIndex()
            {
                Map = persons => from person in persons
                    select new
                    {
                        person.LastName
                    };
            }

            public class Result
            {
                public string LastName { get; set; }
            }
        }

        private class PersonWithCompanyIndex : AbstractIndexCreationTask<Person>
        {
            public PersonWithCompanyIndex()
            {
                Map = persons => from person in persons
                    let company = LoadDocument<Company>( person.CompanyReference.Id ) // THIS CAUSES EVERYTHING TO WORK!
                    select new
                    {
                        person.LastName
                    };
            }

            public class Result
            {
                public string LastName { get; set; }
            }
        }

        private class PersonCompanyProjection
        {
            public string PersonId { get; set; }
            public string CompanyName { get; set; }
        }
    }
}
