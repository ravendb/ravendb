using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Embedded;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5513: RavenTest
    {
        [Fact]
        public async Task CanUseEmbeddedFileSystemWithoutSettingDefaultName()
        {
            using (EmbeddableDocumentStore store = NewDocumentStore())
            {
                await store.FilesStore.AsyncFilesCommands.UploadAsync("test.dat", new MemoryStream(new byte[] { 1, 2, 3 }));
                var metadata = await store.FilesStore.AsyncFilesCommands.GetMetadataForAsync("test.dat");
                Assert.NotNull(metadata);
            }
        }
    }
}
