// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2124.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Client;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2124 : RavenTest
    {
        [Fact]
        public void IncludeWithBadClrTypeShouldWorkForBothLoadsAndQueries()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.Put("addresses/1", null, RavenJObject.FromObject(new Address { Street = "Street1" }), new RavenJObject());

                using (var session = store.OpenSession())
                {
                    var person = new Person { AddressId = "addresses/1", Name = "Name1" };
                    session.Store(person);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var person = session.Include<Person>(x => x.AddressId).Load<Person>("people/1");

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
