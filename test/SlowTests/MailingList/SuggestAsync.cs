// -----------------------------------------------------------------------
//  <copyright file="SuggestAsync.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class SuggestAsync : RavenTestBase
    {
        public SuggestAsync(ITestOutputHelper output) : base(output)
        {
        }

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

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task DoWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new People_ByName().Execute(store);
                using (IAsyncDocumentSession session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Person { Name = "Jack", Age = 20 });
                    await session.StoreAsync(new Person { Name = "Steve", Age = 74 });
                    await session.StoreAsync(new Person { Name = "Martin", Age = 34 });
                    await session.StoreAsync(new Person { Name = "George", Age = 12 });

                    await session.SaveChangesAsync();

                    var query = session.Query<Person, People_ByName>()
                        .SuggestUsing(x => x.ByField(y => y.Name, "martin"));

                    await query.ExecuteAsync();
                }
            }
        }
    }
}
