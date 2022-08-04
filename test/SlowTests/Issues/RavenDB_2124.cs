// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2124.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2124 : RavenTestBase
    {
        public RavenDB_2124(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void IncludeWithBadClrTypeShouldWorkForBothLoadsAndQueries(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var commands = store.Commands())
                {
                    commands.Put("addresses/1", null, new Address { Street = "Street1" });
                }

                using (var session = store.OpenSession())
                {
                    var person = new Person { AddressId = "addresses/1", Name = "Name1" };
                    session.Store(person);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var person = session.Include<Person>(x => x.AddressId).Load<Person>("people/1-A");

                    Assert.NotNull(person);
                    Assert.Equal("Name1", person.Name);
                    Assert.Equal("addresses/1", person.AddressId);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var address = session.Load<Address>(person.AddressId);

                    Assert.Equal("Street1", address.Street);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }

                using (var session = store.OpenSession())
                {
                    var people = session
                        .Query<Person>()
                        .Include(x => x.AddressId)
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(1, people.Count);

                    var person = people[0];

                    Assert.NotNull(person);
                    Assert.Equal("Name1", person.Name);
                    Assert.Equal("addresses/1", person.AddressId);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var address = session.Load<Address>(person.AddressId);

                    Assert.Equal("Street1", address.Street);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
