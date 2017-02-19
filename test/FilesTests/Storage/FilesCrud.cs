using System.IO;
using FastTests;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Files;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;

namespace FilesTests.Storage
{
    public class FilesCrud : RavenLowLevelTestBase
    {
        private RavenConfiguration _configuration;
        private readonly FileSystem _fileSystem;

        public FilesCrud()
        {
            _configuration = new RavenConfiguration("foo", ResourceType.FileSystem);
            _configuration.Initialize();

            _configuration.Core.RunInMemory = true;
            _configuration.Core.DataDirectory = new PathSetting(Path.GetTempPath() + @"\files\crud");

            _fileSystem = new FileSystem("foo", _configuration, null);
            _fileSystem.Initialize();
        }

        [Fact]
        public void PutAndGetFileByName()
        {
            var name = "fileNANE_#$1^%_בעברית.txt";

            using (var ctx = FilesOperationContext.ShortTermSingleUse(_fileSystem))
            {
                ctx.OpenWriteTransaction();

                using (var file = new MemoryStream(new byte[] {1,2,3,4,5}))
                {
                    var metadata = ctx.ReadObject(new DynamicJsonValue
                    {
                        ["Some"] = "Value"
                    }, name);
                    _fileSystem.FilesStorage.Put(ctx, name, null, file, metadata);
                }
                ctx.Transaction.Commit();
            }

            using (var ctx = FilesOperationContext.ShortTermSingleUse(_fileSystem))
            {
                ctx.OpenReadTransaction();

                var file = _fileSystem.FilesStorage.Get(ctx, name);
                var stream = _fileSystem.FilesStorage.GetStream(ctx, file.StreamIdentifier);
                Assert.NotNull(file);
                Assert.Equal(1, file.Etag);
                Assert.Equal(name, file.Name);
                Assert.Equal(5, stream.Length);
                var readBuffer = new byte[5];
                Assert.Equal(5, stream.Read(readBuffer, 0, 5));
                Assert.Equal(new byte[] { 1, 2, 3, 4, 5 }, readBuffer);
            }
        }

        public override void Dispose()
        {
            _fileSystem.Dispose();

            base.Dispose();
        }
    }
}