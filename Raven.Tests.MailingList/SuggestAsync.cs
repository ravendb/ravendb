// -----------------------------------------------------------------------
//  <copyright file="SuggestAsync.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class SuggestAsync : RavenTest
    {
        public class Person
        {
            public string Name;
            public int Age;
        }

        public class People_ByName : AbstractIndexCreationTask<Person>
        {
            public People_ByName()
            {
                Map = people =>
                      from person in people
                      select new {person.Name};

                Suggestion(x=>x.Name, new SuggestionOptions
                {
                    Accuracy = 0.5f,
                    Distance = StringDistanceTypes.Default
                });
            }
        }

        [Fact]
        public void DoWork()
        {
            using (var store = NewDocumentStore())
            {
                new People_ByName().Execute(store);
                using (IAsyncDocumentSession session = store.OpenAsyncSession())
                {
                    session.StoreAsync(new Person {Name = "Jack", Age = 20}).Wait();
                    session.StoreAsync(new Person {Name = "Steve", Age = 74}).Wait();
                    session.StoreAsync(new Person {Name = "Martin", Age = 34}).Wait();
                    session.StoreAsync(new Person {Name = "George", Age = 12}).Wait();

                    session.SaveChangesAsync().Wait();


                    IRavenQueryable<Person> query = session.Query<Person, People_ByName>()
                                                           .Search(p => p.Name, "martin");

                    query.SuggestAsync().Wait();

                }
            }
        }
    }
}