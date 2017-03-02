using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB1009 : RavenTestBase
    {
        private class Foo
        {
            public byte[] Hash { get; set; }
        }

        [Fact]
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
