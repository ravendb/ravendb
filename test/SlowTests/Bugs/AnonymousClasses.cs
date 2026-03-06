using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Bugs
{
    public class AnonymousClasses : RavenTestBase
    {
        public AnonymousClasses(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void WillNotCreateNastyIds()
        {
            using(var store = GetDocumentStore())
            {
                using(var s = store.OpenSession())
                {
                    var entity = new {a = 1};
                    s.Store(entity);
                    s.SaveChanges();

                    string id = s.Advanced.GetDocumentId(entity);

                    Assert.DoesNotContain("anonymoustype", id);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void WillNotSetCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    var entity = new { a = 1 };
                    s.Store(entity);

                    var metadata = s.Advanced.GetMetadataFor(entity);
                    Assert.False(metadata.ContainsKey("@collection"));

                    s.SaveChanges();

                    metadata = s.Advanced.GetMetadataFor(entity);
                    Assert.Equal("@empty", metadata.GetString("@collection"));
                }
            }
        }
    }
}