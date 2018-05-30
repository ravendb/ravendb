
using FastTests;
using Xunit;

namespace SlowTests.Blittable
{
    public class RavenDB_11243 : RavenTestBase
    {
        [Fact]
        public void CanSerializeStringPropertyOfExactly327666Chars()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new
                    {
                        Txt = new string(' ', 32766)
                    });
                    session.SaveChanges();
                }
            }
        }
    }
}
