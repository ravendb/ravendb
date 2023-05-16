using FastTests;
using Xunit;
using System.Linq;
using Raven.Client.Documents;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Queries
{
    public class WithAs : RavenTestBase
    {
        public WithAs(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void WillAutomaticallyGenerateSelect(Options options)
        {
            using(var store = GetDocumentStore(options))
            {
                using(var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Age = 15,
                        Email = "ayende"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var array = session.Query<User>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .ProjectInto<AgeAndEmail>()
                        .ToArray();

                    Assert.Equal(1, array.Length);
                    Assert.Equal(15, array[0].Age);
                    Assert.Equal("ayende", array[0].Email);
                }
            }
        }

        private class AgeAndEmail
        {
            public int Age { get; set; }
            public string Email { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }
    }
}
