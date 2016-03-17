using Raven.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Database.Extensions;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4240 : RavenFilesTestBase
    {
        private readonly string backupLocation =
            Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Guid.NewGuid().ToString());

        [Fact]
        public async Task BackupFromInMemoryStoreShouldWork()
        {
            // ReSharper disable once RedundantArgumentDefaultValue
            using (var store = NewStore(requestedStorage: "voron", runInMemory: true))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test.file", new MemoryStream(new byte[] { 1, 2, 3 }));
                    await session.SaveChangesAsync();
                }

                await store.AsyncFilesCommands.Admin.StartBackup(backupLocation,
                    null, false, store.DefaultFileSystem);
                WaitForBackup(store.AsyncFilesCommands, true);
            }
        }

        public override void Dispose()
        {
            base.Dispose();
            IOExtensions.DeleteDirectory(backupLocation);
        }
    }
}
