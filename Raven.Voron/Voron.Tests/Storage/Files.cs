// -----------------------------------------------------------------------
//  <copyright file="Files.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

using Xunit;

namespace Voron.Tests.Storage
{
    public class Files : StorageTest
    {
        private readonly string path;

        private readonly string temp;

        public Files()
        {
            path = Path.GetFullPath("Data");
            temp = Path.GetFullPath("Temp");

            DeleteDirectory(path);
            DeleteDirectory(temp);
        }

        [Fact]
        public void ByDefaultAllFilesShouldBeStoredInOneDirectory()
        {
            var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(path);

            Assert.Equal(path, options.BasePath);
            Assert.Equal(options.BasePath, options.TempPath);
        }

        [Fact]
        public void TemporaryPathTest()
        {
            var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(path, temp);

            Assert.Equal(path, options.BasePath);
            Assert.Equal(temp, options.TempPath);
        }

        [Fact]
        public void DefaultScratchLocation()
        {
            var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(path);
            using (var env = new StorageEnvironment(options))
            {
                var scratchFile = Path.Combine(path, "scratch.buffers");
                Assert.True(File.Exists(scratchFile));
            }
        }

        [Fact]
        public void ScratchLocationWithTemporaryPathSpecified()
        {
            var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(path, temp);
            using (var env = new StorageEnvironment(options))
            {
                var scratchFile = Path.Combine(path, "scratch.buffers");
                var scratchFileTemp = Path.Combine(temp, "scratch.buffers");

                Assert.False(File.Exists(scratchFile));
                Assert.True(File.Exists(scratchFileTemp));
            }
        }

        public override void Dispose()
        {
            DeleteDirectory(path);
            DeleteDirectory(temp);

            base.Dispose();
        }
    }
}