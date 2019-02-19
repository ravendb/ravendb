using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Exceptions.Documents.Compilation;
using Xunit;

namespace SlowTests.MailingList
{
    public class Justin : RavenTestBase
    {
        [Fact]
        public void ActualTest()
        {
            // Arrange.
            using (var store = GetDocumentStore())
            {

                new Users_NameAndPassportSearching().Execute(store);

                var users = CreateFakeUsers();
                var usersCount = users.Count();
                using (var documentSession = store.OpenSession())
                {
                    foreach (var user in users)
                    {
                        documentSession.Store(user);
                    }
                    documentSession.SaveChanges();
                }


                // If we want to search for *Krome .. this means the index will contain
                // 'emorK eruP' .. so we need to reverse the search query string.
                var userSearchQuery = new string("Krome".Reverse().ToArray());
                var passportSearchQuery = new string("12345".Reverse().ToArray());


                // Act.

                // Lets check if there are any errors.
                RavenTestHelper.AssertNoIndexErrors(store);

                using (var documentSession = store.OpenSession())
                {
                    var allData = documentSession
                        .Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(usersCount, allData.Count);

                    var specificUsers = documentSession
                        .Query<Users_NameAndPassportSearching.ReduceResult, Users_NameAndPassportSearching>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.ReversedName.StartsWith(userSearchQuery))
                        .As<User>()
                        .ToList();

                    var passports = documentSession
                        .Query<Users_NameAndPassportSearching.ReduceResult, Users_NameAndPassportSearching>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.ReversedName.StartsWith(passportSearchQuery))
                        .ToList();

                }
            }
        }

        [Fact]
        public void ActualTest_IgnoreErrors()
        {
            // Arrange.
            using (var documentStore = GetDocumentStore())
            {

                Assert.Throws<IndexCompilationException>(() => new Users_NameAndPassportSearching_WithError().Execute(documentStore));

                var users = CreateFakeUsers();
                var usersCount = users.Count();
                using (var documentSession = documentStore.OpenSession())
                {
                    foreach (var user in users)
                    {
                        documentSession.Store(user);
                    }
                    documentSession.SaveChanges();
                }

                // Act.

                // Lets check if there are any errors.
                RavenTestHelper.AssertNoIndexErrors(documentStore);

                using (var documentSession = documentStore.OpenSession())
                {
                    var allData = documentSession
                        .Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(usersCount, allData.Count);
                }

                // Assert.
            }
        }

        private static IEnumerable<User> CreateFakeUsers()
        {
            return new List<User>
                       {
                           new User
                               {
                                   Name = "Pure Krome",
                                   Age = 36,
                                   PassportNumber = "QWERTY-12345"
                               },
                           new User
                               {
                                   Name = "Ayende Rayen",
                                   Age = 35,
                                   PassportNumber = "ABC-12345"
                               },
                           new User
                               {
                                   Name = "Itamar Syn-Hershko",
                                   Age = 34,
                                   PassportNumber = "DEF-12345"
                               },
                           new User
                               {
                                   Name = "aaa bbb",
                                   Age = 33,
                                   PassportNumber = "GHI-12345"
                               },
                           new User
                               {
                                   Name = "ccc ddd",
                                   Age = 32,
                                   PassportNumber = "JKL-12345"
                               },
                           new User
                               {
                                   Name = "eee fff",
                                   Age = 31,
                                   PassportNumber = "MNO-12345"
                               }
                       };
        }

        private class User
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public string PassportNumber { get; set; }
        }

        private class Users_NameAndPassportSearching_WithError : AbstractIndexCreationTask<User, Users_NameAndPassportSearching.ReduceResult>
        {
            public Users_NameAndPassportSearching_WithError()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name,
                                   user.PassportNumber,
                                   ReversedName = user.Name,
                                   ReversedPassportNumber = user.PassportNumber,
                               };

                // This result function will cause RavenDB to throw an error
                Reduce = results => from r in results
                                    select new
                                    {
                                        r.Name,
                                        r.PassportNumber,
                                        ReversedName = r.Name,
                                    };
            }
        }

        private class Users_NameAndPassportSearching : AbstractIndexCreationTask<User, Users_NameAndPassportSearching.ReduceResult>
        {
            public Users_NameAndPassportSearching()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name,
                                   user.PassportNumber,
                                   ReversedName = user.Name.Reverse(),
                                   ReversedPassportNumber = user.PassportNumber.Reverse(),
                               };
            }

            #region Nested type: ReduceResult

            public class ReduceResult
            {
                public string Name { get; set; }
                public string PassportNumber { get; set; }
                public string ReversedName { get; set; }
                public string ReversedPassportNumber { get; set; }
            }

            #endregion
        }
    }
}
