using FastTests;
using Xunit;
using System.Linq;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Metadata
{
    public class Querying : RavenTestBase
    {
        public Querying(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_query_metadata(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var user1 = new User { Name = "Joe Schmoe" };
                // This test succeeds if I use "Test-Property1" as the  property name.
                const string propertyName1 = "Test-Property-1";
                const string propertyValue1 = "Test-Value-1";
                using (var session = store.OpenSession())
                {
                    session.Store(user1);
                    var metadata1 = session.Advanced.GetMetadataFor(user1);
                    metadata1[propertyName1] = propertyValue1;
                    session.Store(new User { Name = "Ralph Schmoe" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.DocumentQuery<User>()
                        .WaitForNonStaleResults()
                        .WhereEquals("@metadata." + propertyName1, propertyValue1)
                        .ToList();

                    Assert.NotNull(result);
                    Assert.Equal(1, result.Count);
                    var metadata = session.Advanced.GetMetadataFor(result[0]);
                    Assert.Equal(propertyValue1, metadata[propertyName1]);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Index_should_take_into_account_number_of_dashes(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var user1 = new User { Name = "Joe Schmoe" };
                using (var session = store.OpenSession())
                {
                    session.Store(user1);
                    var metadata1 = session.Advanced.GetMetadataFor(user1);
                    metadata1["Test-Property-1"] = "Test-Value-1";
                    session.Store(new User { Name = "Ralph Schmoe" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Empty(session.Advanced.DocumentQuery<User>()
                                    .WaitForNonStaleResults()
                                    .WhereEquals("@metadata.'Test-Property1'", "Test-Value-1")
                                    .ToList());

                    var result = session.Advanced.DocumentQuery<User>()
                        .WaitForNonStaleResults()
                        .WhereEquals("@metadata.'Test-Property-1'", "Test-Value-1")
                        .ToList();

                    Assert.NotNull(result);
                    Assert.Equal(1, result.Count);
                    var metadata = session.Advanced.GetMetadataFor(result[0]);
                    Assert.Equal("Test-Value-1", metadata["Test-Property-1"]);
                }
            }
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
