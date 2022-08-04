using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class LoadAllStartingWith : RavenTestBase
    {
        public LoadAllStartingWith(ITestOutputHelper output) : base(output)
        {
        }

        private class Abc
        {
            public string Id { get; set; }
        }

        private class Xyz
        {
            public string Id { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void LoadAllStartingWithShouldNotLoadDeletedDocs(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var doc1 = new Abc
                {
                    Id = "abc/1",
                };
                var doc2 = new Xyz
                {
                    Id = "xyz/1"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(doc1);
                    session.Store(doc2);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // commenting out this call passes the test
                    var testClasses = session.Advanced.Lazily.LoadStartingWith<Abc>("abc/");
                    var test2Classes = session.Query<Xyz>().Customize(x => x.WaitForNonStaleResults())
                                              .Lazily().Value.ToList();

                    Assert.Equal(1, testClasses.Value.Count());
                    Assert.Equal(1, test2Classes.Count());
                }
            }
        }
    }
}
