using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16902 : RavenTestBase
    {
        public RavenDB_16902(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData("\u0002", DatabaseMode = RavenDatabaseMode.All)]
        [RavenData("ab\0ac", DatabaseMode = RavenDatabaseMode.All)]
        public void DocumentWithUnicodeCharacterShouldNotHaveChangesOnLoad(Options options, string description)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc {Id = "doc/1", Description = description };
                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDoc>("doc/1");

                    Assert.False(session.Advanced.HasChanges);
                    Assert.False(session.Advanced.HasChanged(doc));
                    Assert.True(doc.Description.Equals(description));
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void DocumentWithUnicodeCharacterShouldNotHaveChangesOnLoad()
        {
            // compressed string case does not affect the comparison and should work as well 
            var description = new string('\u0003', 4096);

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var doc = new TestDoc { Id = "doc/1", Description = description };
                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<TestDoc>("doc/1");

                    Assert.False(session.Advanced.HasChanges);
                    Assert.False(session.Advanced.HasChanged(doc));
                    Assert.True(doc.Description.Equals(description));
                }
            }
        }
    }

    internal class TestDoc  
    {
        public string Id { get; set; }
        public string Description { get; set; }
    }
}
