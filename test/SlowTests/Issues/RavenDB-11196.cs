using System;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Documents.ETL;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11196 : EtlTestBase
    {
        [Fact]
        public void Should_be_James()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script:
                    @"
this.Name = 'James';

loadToUsers(this);

var person = { Name: this.Name };

loadToPeople(person);
"
                );

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe",
                        LastName = "Doe",
                    }, "users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(100));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("James", user.Name);

                    var person = session.Advanced.LoadStartingWith<Person>("users/1/people/")[0];

                    Assert.NotNull(person);
                    Assert.Equal("James", person.Name); // throws here, actual: Joe
                }
            }
        }
    }
}
