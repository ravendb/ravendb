using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Json.Linq;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_3499 : RavenFilesTestWithLogs
    {
        [Theory]
        [PropertyData("Storages")]
        public async Task TestLongEsentFileName(string requestedStorage)
        {
            using (var db = NewStore(requestedStorage: requestedStorage))
            {
                var longpath = "mycollection/4942d4c8151a416a88d8b0f3e783b443/attachments/96cf29b3af625f5fe9d9ae6255a900f179b97c96/This_is_a_really_long_filename_that_breaks_things.pdf";
                var longerPath = longpath + "bar";
                using (var session = db.OpenAsyncSession())
                {
                    session.RegisterUpload(longpath, new MemoryStream(new byte[] {1, 2, 3}), new RavenJObject {{"Test", "1"}});
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }
                using (var session = db.OpenAsyncSession())
                {
                    session.RegisterRename(longpath, longerPath);
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }
                using (var session = db.OpenAsyncSession())
                {
                    session.RegisterUpload(longerPath, new MemoryStream(new byte[] {1, 2, 3}), new RavenJObject {{"Test", "1"}});
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }
                using (var session = db.OpenAsyncSession())
                {
                    session.RegisterFileDeletion(longerPath);
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }
            }
        }

        [Fact]
        public void ValidateTooLongPath()
        {
            using (var db = NewStore())
            {
                var client = db.AsyncFilesCommands;
                var stringParts = Enumerable.Repeat("1234567\\", 125).ToList();
                stringParts[stringParts.Count - 1] = "12345678";
                var longPath = string.Concat(stringParts);

                client.UploadAsync(longPath, new MemoryStream(new byte[] {1, 2, 3}), new RavenJObject {{"Test", "1"}});

                longPath = string.Concat(longPath, "1");
                Assert.Throws<AggregateException>(() =>
                {
                    client.UploadAsync(longPath, new MemoryStream(new byte[] {1, 2, 3}), new RavenJObject {{"Test", "1"}}).Wait();
                    return null;
                });

                var shortName = "file.file";
                client.UploadAsync(shortName, new MemoryStream(new byte[] {1, 2, 3}), new RavenJObject {{"Test", "1"}}).Wait();
                Assert.Throws<AggregateException>(() =>
                {
                    client.RenameAsync(shortName, longPath).Wait();
                    return null;
                });
            }
        }
    }
}
