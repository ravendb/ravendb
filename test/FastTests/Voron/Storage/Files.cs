// -----------------------------------------------------------------------
//  <copyright file="Files.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Xunit;
using Voron;
 
namespace FastTests.Voron.Storage
{
    public class Files : StorageTest
    {
        
        [Fact]
        public void ByDefaultAllFilesShouldBeStoredInOneDirectory()
        {
            var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(DataDir);

            Assert.Equal(DataDir, options.BasePath);
            Assert.Equal(options.BasePath, options.TempPath);
        }

        [Fact]
        public void TemporaryPathTest()
        {
            var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(DataDir, DataDir + "Temp");

            Assert.Equal(DataDir, options.BasePath);
            Assert.Equal(DataDir + "Temp", options.TempPath);
        }

        [Fact]
        public void DefaultScratchLocation()
        {
            var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(DataDir);
            using (var env = new StorageEnvironment(options))
            {
                var scratchFile = Path.Combine(DataDir, StorageEnvironmentOptions.ScratchBufferName(0));
                Assert.True(File.Exists(scratchFile));
            }
        }

        [Fact]
        public void ScratchLocationWithTemporaryPathSpecified()
        {
            var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(DataDir, DataDir + "Temp");
            using (var env = new StorageEnvironment(options))
            {
                var scratchFile = Path.Combine(DataDir, StorageEnvironmentOptions.ScratchBufferName(0));
                var scratchFileTemp = Path.Combine(DataDir +"Temp", StorageEnvironmentOptions.ScratchBufferName(0));

                Assert.False(File.Exists(scratchFile));
                Assert.True(File.Exists(scratchFileTemp));
            }
        }

        public override void Dispose()
        {
            DeleteDirectory(DataDir+"Temp");

            base.Dispose();
        }
    }
}
