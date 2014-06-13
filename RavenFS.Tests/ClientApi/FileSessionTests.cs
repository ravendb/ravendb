using Raven.Client.FileSystem;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace RavenFS.Tests.ClientApi
{
    public class FileSessionTests : RavenFsTestBase
    {

        private readonly IFilesStore filesStore;

        public FileSessionTests()
		{
			filesStore = this.NewStore();
		}

        [Fact]
        public void SessionLifecycle ()
        {
            var store = (FilesStore) filesStore;

            using( var session = filesStore.OpenAsyncSession())
            {
                Assert.NotNull(session.Advanced);
                Assert.True(session.Advanced.MaxNumberOfRequestsPerSession == 30);
                Assert.False(string.IsNullOrWhiteSpace(session.Advanced.StoreIdentifier));
                Assert.Equal(filesStore, session.Advanced.FilesStore);
                Assert.Equal(filesStore.Identifier, session.Advanced.StoreIdentifier.Split(';')[0]);
                Assert.Equal(store.DefaultFileSystem, session.Advanced.StoreIdentifier.Split(';')[1]);
            }
        }

    }
}
