// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2325.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Transformers;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2325 : RavenTestBase
    {
        private class UserAge : AbstractTransformerCreationTask<User>
        {
            public UserAge()
            {
                TransformResults = users => from user in users
                                            select user.Age;
            }
        }

        private class PersonAddress : AbstractTransformerCreationTask<PersonWithAddress>
        {
            public PersonAddress()
            {
                TransformResults = people => from person in people
                                             select person.Address;
            }
        }

        [Fact]
        public void IfTransformerReturnsNullItShouldNotThrow()
        {
            using (var store = GetDocumentStore())
            {
                new UserAge().Execute(store);
                new PersonAddress().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Age = 10 });
                    session.Store(new Company { Name = "Name1" });
                    session.Store(new PersonWithAddress
                    {
                        Name = "Person1",
                        Address = new Address
                        {
                            Street = "Street1"
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results1 = session
                        .Query<Company>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .TransformWith<UserAge, long?>()
                        .ToList();

                    Assert.Equal(1, results1.Count);
                    Assert.Equal(null, results1[0]);

                    results1 = session
                        .Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .TransformWith<UserAge, long?>()
                        .ToList();

                    Assert.Equal(1, results1.Count);
                    Assert.Equal(10, results1[0]);

                    var results2 = session
                        .Query<Company>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .TransformWith<PersonAddress, Address>()
                        .ToList();

                    Assert.Equal(1, results2.Count);
                    Assert.Equal(null, results2[0]);

                    results2 = session
                        .Query<PersonWithAddress>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .TransformWith<PersonAddress, Address>()
                        .ToList();

                    Assert.Equal(1, results2.Count);
                    Assert.Equal("Street1", results2[0].Street);
                }
            }
        }
    }
}
