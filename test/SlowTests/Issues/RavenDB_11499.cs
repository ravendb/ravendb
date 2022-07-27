using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11499 : RavenTestBase
    {
        public RavenDB_11499(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public sbyte Sbyte { get; set; }

            public ushort Ushort { get; set; }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanPatchSbyteAndUshort(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Sbyte = 33,
                        Ushort = 55
                    }, "users/1");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<User, sbyte>("users/1", u => u.Sbyte, 22);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Patch<User, ushort>("users/1", u => u.Ushort, 11);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.Equal((sbyte)22, user.Sbyte);
                    Assert.Equal((ushort)11, user.Ushort);
                }
            }
        }
    }
}
