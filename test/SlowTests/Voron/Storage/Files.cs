// -----------------------------------------------------------------------
//  <copyright file="Files.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Storage
{
    public class Files : FastTests.Voron.StorageTest
    {
        public Files(ITestOutputHelper output) : base(output)
        {
        }


        [RavenFact(RavenTestCategory.Voron)]
        public void ByDefaultAllFilesShouldBeStoredInOneDirectory()
        {
            var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPathForTests(DataDir);

            Assert.Equal(DataDir, options.BasePath.FullPath);
            Assert.True(options.TempPath.FullPath.StartsWith(options.BasePath.FullPath));
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void TemporaryPathTest()
        {
            var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(DataDir, DataDir + "Temp", null, null, null, null, null);

            Assert.Equal(DataDir, options.BasePath.FullPath);
            Assert.Equal(DataDir + "Temp", options.TempPath.FullPath);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void DefaultScratchLocation()
        {
            var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPathForTests(DataDir);
            using (var env = new StorageEnvironment(options))
            {
                var scratchFile = Path.Combine(env.Options.TempPath.FullPath, StorageEnvironmentOptions.ScratchBufferName(0));
                Assert.True(File.Exists(scratchFile));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ScratchLocationWithTemporaryPathSpecified()
        {
            var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)StorageEnvironmentOptions.ForPath(DataDir, DataDir + "Temp", null, null, null, null, null);
            using (var env = new StorageEnvironment(options))
            {
                var scratchFile = Path.Combine(DataDir, StorageEnvironmentOptions.ScratchBufferName(0));
                var scratchFileTemp = Path.Combine(DataDir + "Temp", StorageEnvironmentOptions.ScratchBufferName(0));

                Assert.False(File.Exists(scratchFile));
                Assert.True(File.Exists(scratchFileTemp));
            }
        }

        public override void Dispose()
        {
            IOExtensions.DeleteDirectory(DataDir + "Temp");

            base.Dispose();
        }
    }
}
