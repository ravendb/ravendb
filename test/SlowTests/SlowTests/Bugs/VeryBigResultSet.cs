using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.SlowTests.Bugs
{
    public class VeryBigResultSet : RavenTestBase
    {
        [Fact]
        public void CanGetVeryBigResultSetsEvenThoughItIsBadForYou()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 15000; i++)
                    {
                        session.Store(new User());
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>()
                                       .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(1)))
                                       .Take(20000)
                                       .ToArray();

                    try
                    {
                        Assert.Equal(15000, users.Length);
                    }
                    catch (Exception)
                    {
                        PrintMissedDocuments(users);
                        throw;
                    }
                }
            }
        }

        private static void PrintMissedDocuments(User[] users)
        {
            var missed = new List<int>();
            for (int i = 0; i < 15000; i++)
            {
                if (users.Any(user => user.Id == (i + 1).ToString()) == false)
                {
                    missed.Add(i + 1);
                }
            }
            Console.WriteLine("Missed documents: ");
            Console.WriteLine(string.Join(", ", missed));
        }

        private class User
        {
            public string Id { get; set; }
        }
    }
}
