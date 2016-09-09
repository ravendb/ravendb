// -----------------------------------------------------------------------
//  <copyright file="SuggestAsync.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class SuggestAsync : RavenTestBase
    {
        private class Person
        {
            public string Name;
#pragma warning disable 414
            public int Age;
#pragma warning restore 414
        }

        private class People_ByName : AbstractIndexCreationTask<Person>
        {
            public People_ByName()
            {
                Map = people =>
                      from person in people
                      select new { person.Name };

                Suggestion(x => x.Name);
            }
        }

        [Fact(Skip = "Missing feature: Suggestions")]
        public void DoWork()
        {
            using (var store = GetDocumentStore())
            {
                new People_ByName().Execute(store);
                using (IAsyncDocumentSession session = store.OpenAsyncSession())
                {
                    session.StoreAsync(new Person { Name = "Jack", Age = 20 }).Wait();
                    session.StoreAsync(new Person { Name = "Steve", Age = 74 }).Wait();
                    session.StoreAsync(new Person { Name = "Martin", Age = 34 }).Wait();
                    session.StoreAsync(new Person { Name = "George", Age = 12 }).Wait();

                    session.SaveChangesAsync().Wait();


                    IRavenQueryable<Person> query = session.Query<Person, People_ByName>()
                                                           .Search(p => p.Name, "martin");

                    query.SuggestAsync().Wait();
                }
            }
        }
    }
}
