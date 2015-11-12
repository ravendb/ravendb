using System.Net;
using Raven.Client.FileSystem;
using Xunit;

namespace Raven.Tests.Issues
{

    public class RavenDB_3570
    {
        [Fact]
        public void CredentialsAreLoadedFromConnectionString()
        {
            using (var store = new FilesStore()
            {
                ConnectionStringName = "RavenFS"
            })
            {
                var credentials = (NetworkCredential)store.Credentials;

                Assert.Equal("local_user_test", credentials.UserName);
                Assert.Equal("local_user_test", credentials.Password);
                Assert.Equal(string.Empty, credentials.Domain);
            }
        }
    }
}
