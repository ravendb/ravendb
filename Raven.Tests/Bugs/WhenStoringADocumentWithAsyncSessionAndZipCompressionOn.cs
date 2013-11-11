using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class WhenStoringADocumentWithAsyncSessionAndZipCompression : RavenTest
    {
        [Fact]
        public async Task IsOn()
        {
            using (var store = NewRemoteDocumentStore(databaseName: "TEST-ASYNC"))
            {
                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = "john"
                    };
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task IsOff()
        {
            using (var store = NewRemoteDocumentStore(databaseName: "TEST-ASYNC"))
            {
                // http://stackoverflow.com/questions/13859467/ravendb-client-onlinux-connecting-to-windows-server-using-mono-http
                store.JsonRequestFactory.DisableRequestCompression = true;

                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = "john"
                    };
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public void IsOffSync()
        {
            using (var store = NewRemoteDocumentStore(databaseName: "TEST-ASYNC"))
            {
                // http://stackoverflow.com/questions/13859467/ravendb-client-onlinux-connecting-to-windows-server-using-mono-http
                store.JsonRequestFactory.DisableRequestCompression = true;

                using (var session = store.OpenSession())
                {
                    var user = new User
                    {
                        Name = "john"
                    };
                    session.Store(user);
                    session.SaveChanges();
                }
            }
        }

        public class User
        {
            public string Name { get; set; }
        }
    }
}