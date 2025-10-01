using FastTests;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Issues
{
    public class RavenDB1009 : RavenTestBase
    {
        public RavenDB1009(ITestOutputHelper output) : base(output)
        {
        }

        private class Foo
        {
            public byte[] Hash { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void CanHandleWhenSettingByteArrayToNull()
        {
            using (var store = GetDocumentStore())
            {
                // store a doc
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo(), "foos/1");
                    session.SaveChanges();
                }

                // store a doc
                using (var session = store.OpenSession())
                {
                    var foo = session.Load<Foo>("foos/1");
                    foo.Hash = new byte[100];
                    session.SaveChanges();
                }
            }
        }
    }
}
