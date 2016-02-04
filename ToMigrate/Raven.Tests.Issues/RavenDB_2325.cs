// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2325.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2325 : RavenTest
    {
        private class UserAge : AbstractTransformerCreationTask<User>
        {
            internal class Result
            {
                public int Value { get; set; }
            }

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
            using (var store = NewDocumentStore())
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
                        .TransformWith<UserAge, UserAge.Result>()
                        .ToList();

                    Assert.Equal(1, results1.Count);
                    Assert.Equal(null, results1[0]);

                    results1 = session
                        .Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .TransformWith<UserAge, UserAge.Result>()
                        .ToList();

                    Assert.Equal(1, results1.Count);
                    Assert.Equal(10, results1[0].Value);

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
