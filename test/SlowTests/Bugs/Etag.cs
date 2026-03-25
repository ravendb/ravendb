using System;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Bugs
{
    public class ChangeVectorExists : RavenTestBase
    {
        public ChangeVectorExists(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void WhenSaving_ThenGetsChangeVector()
        {
            using (var store = GetDocumentStore())
            {
                var foo = new IndexWithTwoProperties.Foo {Id = Guid.NewGuid().ToString(), Value = "foo"};

                using (var session = store.OpenSession())
                {
                    session.Store(foo);

                    session.SaveChanges();
                    
                    var metadata = session.Advanced.GetMetadataFor(foo);
                    Assert.NotNull(metadata["@change-vector"]);
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<IndexWithTwoProperties.Foo>(foo.Id);

                    var metadata = session.Advanced.GetMetadataFor(loaded);
                    Assert.NotNull(metadata["@change-vector"]);

                }
            }
        }

    }
}