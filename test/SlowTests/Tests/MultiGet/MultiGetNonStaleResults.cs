using System;
using System.Linq;
using Xunit.Abstractions;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Tests.MultiGet
{
    public class MultiGetNonStaleResults : RavenTestBase
    {
        public MultiGetNonStaleResults(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public string Info { get; set; }
            public bool Active { get; set; }
            public DateTime Created { get; set; }

            public User()
            {
                Name = string.Empty;
                Age = default(int);
                Info = string.Empty;
                Active = false;
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldBeAbleToGetNonStaleResults(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "oren")
                        .ToList();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.Store(new User());
                    session.Store(new User { Name = "ayende" });
                    session.Store(new User());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result1 = session.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "oren")
                        .Lazily();

                    Assert.NotEmpty(result1.Value);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
