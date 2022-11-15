using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Indexing;
using Sparrow.Global;
using Sparrow.Server;
using Voron;
using Voron.Exceptions;
using Voron.Util.Settings;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests
{
    public class TempFileCacheTests : RavenTestBase
    {
        public TempFileCacheTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_reuse_files_for_cache()
        {
            var path = new VoronPathSetting(NewDataPath());
            var environment = new StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions(path, path, path, null, null);

            using (File.Create(TempFileCache.GetTempFileName(environment.TempPath.FullPath)))
            {
            }

            using (var cache = new TempFileCache(environment.TempPath.FullPath, environment.Encryption.IsEnabled))
            {
                Assert.Equal(1, cache.FilesCount);
            }
        }

        [Fact]
        public void Skip_files_that_are_in_use()
        {
            var path = new VoronPathSetting(NewDataPath());
            var environment = new StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions(path, path, path, null, null);

            for (var i = 0; i < TempFileCache.MaxFilesToKeepInCache; i++)
            {
                using (File.Create(TempFileCache.GetTempFileName(environment.TempPath.FullPath)))
                {
                }
            }

            using (File.Create(Path.Combine(environment.TempPath.FullPath,
                TempFileCache.FilePrefix + "Z" + StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.TempFileExtension)))
            {
                using (var cache = new TempFileCache(environment.TempPath.FullPath, environment.Encryption.IsEnabled))
                {
                }
            }
        }

        [Fact]
        public void ShouldHaveSeparateFilePositionForDifferentReaders()
        {
            var buffer = new byte[128 * Constants.Size.Kilobyte + 1];
            for (int i = 0; i < buffer.Length; i++)
                buffer[i] = (byte)i;

            var path = Server.Configuration.Storage.TempPath?.FullPath ?? Path.GetTempPath();
            using (var cache = new TempFileCache(path, false))
            {
                var fileStream = cache.RentFileStream();
                fileStream.Write(buffer);
              
                TempFileStream tmp = (TempFileStream)fileStream;
                var reader1 = tmp.CreateReaderStream();
                var reader2 = tmp.CreateReaderStream();

                reader1.Seek(0, SeekOrigin.Begin);
                reader2.Seek(0, SeekOrigin.Begin);

                var buffer1 = new byte[10];
                var buffer2 = new byte[10];

                var read1 = reader1.Read(buffer1);
                var read2 = reader2.Read(buffer2);

                Assert.Equal(read1, read2);
                Assert.Equal(reader1.Position, reader2.Position);
                for (int i = 0; i < read1; i++)
                    Assert.Equal(buffer1[i], buffer2[i]);
               
                reader1.Read(buffer1);
                reader1.Read(buffer1);
                reader2.Read(buffer2);
                
                Assert.NotEqual(reader1.Position, reader2.Position);
            }
        }
    }
}
