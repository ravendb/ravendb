using System.IO;
using FastTests.Voron;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Utils;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10836 : StorageTest
    {
        public RavenDB_10836(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public unsafe void TempCryptoStream_CanWorkWithFilesGreaterThan2GB()
        {
            using (StorageEnvironmentOptions.ForPath(DataDir))
            using (var file = SafeFileStream.Create(Path.Combine(DataDir, "EncryptedTempFile_RavenDB_10836"), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.DeleteOnClose))
            {
                long length = int.MaxValue;

                length += 4096;

                file.SetLength(length);

                using (var stream = new TempCryptoStream(file))
                {
                    var bytes = new byte[4096];

                    fixed (byte* b = bytes)
                    {
                        Memory.Set(b, (byte)'I', bytes.Length);
                    }

                    stream.Write(bytes, 0, bytes.Length);

                    stream.Position = length - 4096 + 1;

                    stream.Write(bytes, 0, bytes.Length);

                    stream.Seek(0, SeekOrigin.Begin);

                    var readBytes = new byte[bytes.Length];

                    var read = stream.Read(readBytes, 0, readBytes.Length);

                    Assert.Equal(4096, read);
                }
            }
        }
    }
}
