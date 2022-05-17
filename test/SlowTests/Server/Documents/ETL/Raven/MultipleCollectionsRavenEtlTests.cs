using Tests.Infrastructure;
using System;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class MultipleCollectionsRavenEtlTests : EtlTestBase
    {
        public MultipleCollectionsRavenEtlTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void Docs_from_two_collections_loaded_to_single_one(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore(options))
            {
                AddEtl(src, dest, new [] { "Users", "People" }, script: @"loadToUsers({Name: this.Name});");

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Joe Doe"
                    },"users/1");

                    session.Store(new Person
                    {
                        Name = "James Smith"
                    },"people/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("Joe Doe", user.Name);

                    var userFromPerson = session.Advanced.LoadStartingWith<User>("people/1/users/")[0];

                    Assert.NotNull(userFromPerson);
                    Assert.Equal("James Smith", userFromPerson.Name);
                }

                // update
                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Doe Joe"
                    }, "users/1");

                    session.Store(new Person
                    {
                        Name = "Smith James"
                    }, "people/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("Doe Joe", user.Name);

                    var userFromPerson = session.Advanced.LoadStartingWith<User>("people/1/users/")[0];

                    Assert.NotNull(userFromPerson);
                    Assert.Equal("Smith James", userFromPerson.Name);
                }

                // delete
                etlDone.Reset();

                using (var session = dest.OpenSession())
                {
                    // this document must not be deleted by ETL

                    session.Store(new Person
                    {
                        Name = "James Smith"
                    }, "people/1");

                    session.SaveChanges();
                }

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");
                    session.Delete("people/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Null(user);

                    var userFromPerson = session.Advanced.LoadStartingWith<User>("people/1/users/");
                    Assert.Equal(0, userFromPerson.Length);

                    var person = session.Load<Person>("people/1");
                    Assert.NotNull(person);
                }
            }
        }
    }
}
