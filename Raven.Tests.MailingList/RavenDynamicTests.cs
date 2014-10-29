// -----------------------------------------------------------------------
//  <copyright file="RavenDynamicTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class RavenDynamicTests
    {
        private static readonly Person Dad = new Person { Name = "Dad" };

	    private static readonly Person Sally = new Person
	    {
		    Name = "sally",
		    UserId = Guid.NewGuid(),
		    Family = new Dictionary<string, Person>
		    {
			    {"Dad", Dad},
		    }
	    };

        public class WhenUsingIdCopy : RavenTest
        {
            private readonly EmbeddableDocumentStore store;

            public WhenUsingIdCopy()
            {
                store = NewDocumentStore();

                new Person_IdCopy_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(Sally);
                    session.Store(new Person { Name = "bob", UserId = Guid.NewGuid() });
                    session.Store(new Person { Name = "stu", UserId = Guid.NewGuid() });

                    session.SaveChanges();
                }
            }

            public override void Dispose()
            {
                store.Dispose();
                base.Dispose();
            }

            [Fact]
            public void It_should_be_stored_in_index()
            {
                using (var session = store.OpenSession())
                {
                    //WaitForUserToContinueTheTest(store);

                    var results2 = session.Advanced.DocumentQuery<Person, Person_IdCopy_Index>()
                                          .WaitForNonStaleResultsAsOfNow()
                                          .SelectFields<PersonIndexItem>()
                                          .ToArray();

                    var s = results2.Single(x => x.Id.Contains("sally"));

                    Assert.Equal(Sally.Family["Dad"].Id, s.Family_Dad_Id);
                }
            }


            [Fact]
            public void It_should_be_stored_be_able_to_be_searched()
            {
                using (var session = store.OpenSession())
                {
                    //WaitForUserToContinueTheTest(store);

                    var results = session.Advanced.DocumentQuery<Person, Person_IdCopy_Index>()
                                         .WaitForNonStaleResultsAsOfNow()
                                         .WhereEquals("Family_Dad_Id", "people/Dad")
                                         .ToArray();


                    Assert.Equal(1, results.Count());
                    Assert.Equal(Sally.Name, results.Single().Name);
                }
            }

            public class Person_IdCopy_Index : AbstractIndexCreationTask<Person>
            {
                public Person_IdCopy_Index()
                {
                    Map = people =>
                          from person in people
                          select new
                          {
                              person.Id,
                              StsId = person.UserId,
                              _ = person.Family.Select(x => CreateField("Family_" + x.Key + "_Id", x.Value.IdCopy, true, true)),
                          };
                }
            }
        }

        public class When_using_Id : RavenTest
        {
            private readonly EmbeddableDocumentStore store;

            public When_using_Id()
            {
                store = NewDocumentStore();

                new Person_Id_Index().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(Sally);
                    session.Store(new Person { Name = "bob", UserId = Guid.NewGuid() });
                    session.Store(new Person { Name = "stu", UserId = Guid.NewGuid() });

                    session.SaveChanges();
                }
            }

            public override void Dispose()
            {
                store.Dispose();
                base.Dispose();
            }

            [Fact]
            public void It_should_be_stored_in_index()
            {
                using (var session = store.OpenSession())
                {
                    //WaitForUserToContinueTheTest(store);

                    var results2 = session.Advanced.DocumentQuery<Person, Person_Id_Index>()
                                          .WaitForNonStaleResultsAsOfNow()
                                          .SelectFields<PersonIndexItem>()
                                          .ToArray();

                    var s = results2.Single(x => x.Id.Contains("sally"));

                    Assert.Equal(Sally.Family["Dad"].Id, s.Family_Dad_Id);
                }
            }


            [Fact]
            public void It_should_be_stored_be_able_to_be_searched()
            {
                using (var session = store.OpenSession())
                {
                    //WaitForUserToContinueTheTest(store);

                    var results = session.Advanced.DocumentQuery<Person, Person_Id_Index>()
                                         .WaitForNonStaleResultsAsOfNow()
                                         .WhereEquals("Family_Dad_Id", "people/Dad")
                                         .ToArray();


                    Assert.Equal(1, results.Count());
                    Assert.Equal(Sally.Name, results.Single().Name);
                }
            }

            public class Person_Id_Index : AbstractIndexCreationTask<Person>
            {
                public Person_Id_Index()
                {
                    Map = people =>
                          from person in people
                          select new
                          {
                              person.Id,
                              StsId = person.UserId,
                              _ = person.Family.Select(x => CreateField("Family_" + x.Key + "_Id", x.Value.Id, true, true)),
                          };
                }
            }
        }

        public class PersonIndexItem
        {
            public string Id { get; set; }
            public string UserId { get; set; }
            public string Family_Dad_Id { get; set; }
        }

        public class Person
        {
            public Person()
            {
                Family = new Dictionary<string, Person>();
            }

            public Guid? UserId { get; set; }

            /// <summary>
            ///     Key is CompanyName from DomainConstants.Companies, Value is upline Agent.
            /// </summary>
            public Dictionary<string, Person> Family { get; set; }

            public string Name { get; set; }


            public string Id
            {
                get { return string.Format("people/{0}", Name); }
            }

            public string IdCopy
            {
                get { return string.Format("people/{0}", Name); }
            }
        }
    }
}