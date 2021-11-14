using FastTests;
using FastTests.Server.JavaScript;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_161 : RavenTestBase
    {
        public RDBC_161(ITestOutputHelper output) : base(output)
        {
        }

        private const string _docId = "users/1-A";

        private class User
        {
            public byte[] Password { get; set; }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanPatchWithByteArray(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                var password = new byte[10];
                password[0] = 1;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Password = password
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    password[0] = 2;
                    // explicitly specify id & type
                    session.Advanced.Patch<User, byte[]>(_docId, u => u.Password, password);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(password, loaded.Password);

                    password[1] = 3;
                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Password, password);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(password, loaded.Password);
                }
            }
        }

    }
}
