using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class Troy2 : RavenTestBase
    {
        [Fact]
        public void UsingDefaultFieldWithSelectFieldsFails()
        {
            using (var store = GetDocumentStore())
            {
                new TesterSearch().Execute(store);
                using (var session = store.OpenSession())
                {

                    var testClasses = new List<Tester>
                    {
                        new Tester
                        {
                            FirstName = "FirstName 1",
                            LastName = "LastName 1",
                            Email = "email1@test.com",
                            Password = "test1"
                        },
                        new Tester
                        {
                            FirstName = "FirstName 2",
                            LastName = "LastName 2",
                            Email = "email2@test.com",
                            Password = "test2"
                        }
                    };
                    testClasses.ForEach(session.Store);
                    session.SaveChanges();

                    QueryStatistics stats;
                    var query = session.Advanced.DocumentQuery<Tester, TesterSearch>()
                                       .WaitForNonStaleResults()
                                       .Statistics(out stats)
                                       .OpenSubclause()
                                       .WhereLucene("Query", "FirstName*")
                                       .CloseSubclause()
                                       .AndAlso()
                                       .WhereEquals(x => x.Email, "email1@test.com")
                                       .OrderBy("Query")
                                       .OrderBy("LastName")
                                       .Skip(0)
                                       .Take(10)
                                       .ToList();
                    Assert.Equal(1, stats.TotalResults);

                    var selectFieldsQuery = session.Advanced.DocumentQuery<Tester, TesterSearch>()
                                   .WaitForNonStaleResults()
                                   .Statistics(out stats)
                                   .OpenSubclause()
                                   .WhereLucene("Query", "FirstName*")
                                   .CloseSubclause()
                                   .AndAlso()
                                   .WhereEquals(x => x.Email, "email1@test.com")
                                   .OrderBy("Query")
                                   .OrderBy("LastName")
                                   .SelectFields<PasswordOnly>()
                                   .Skip(0)
                                   .Take(10)
                                   .ToList();
                    Assert.Equal(1, stats.TotalResults);

                }
            }
        }

        private class TesterSearch : AbstractIndexCreationTask<Tester, TesterSearch.SearchResult>
        {

            public override string IndexName
            {
                get
                {
                    return "Tester/Search";
                }
            }

            public class SearchResult
            {
                public string Query { get; set; }
                public string FirstName { get; set; }
                public string LastName { get; set; }
                public string Email { get; set; }
                public string Password { get; set; }
            }

            public TesterSearch()
            {
                Map = testClasses =>
                        from testClass in testClasses
                        select new
                        {
                            Query = new object[]
                            {
                                testClass.FirstName,
                                testClass.LastName,
                                testClass.Email
                            },
                            testClass.FirstName,
                            testClass.LastName,
                            testClass.Email,
                            testClass.Password
                        };

                Index(x => x.Query, FieldIndexing.Search);
                Index(x => x.FirstName, FieldIndexing.Default);
                Index(x => x.LastName, FieldIndexing.Default);
                Index(x => x.Email, FieldIndexing.Default);
                Index(x => x.Password, FieldIndexing.Default);
            }
        }

        private class PasswordOnly
        {
            public string Password { get; set; }
        }

        private class Tester
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string Email { get; set; }
            public string Password { get; set; }
        }
    }
}
