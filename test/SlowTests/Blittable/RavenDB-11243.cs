
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Blittable
{
    public class RavenDB_11243 : RavenTestBase
    {
        public RavenDB_11243(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
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
