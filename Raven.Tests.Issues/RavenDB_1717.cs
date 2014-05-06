// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1717.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1717 : TransactionalStorageTestBase
    {
        private readonly string path;

        private readonly string temp;

        public RavenDB_1717()
        {
            path = NewDataPath("Data");
            temp = NewDataPath("Temp");
        }

        [Fact]
        public void TempPathForVoronShouldWork1()
        {
            using (var storage = NewTransactionalStorage(requestedStorage: "voron", dataDir: path, runInMemory:false))
            {
                var scratchFile = Path.Combine(path, "scratch.buffers");
                var scratchFileTemp = Path.Combine(temp, "scratch.buffers");

                Assert.True(File.Exists(scratchFile));
                Assert.False(File.Exists(scratchFileTemp));
            }
        }

        [Fact]
        public void TempPathForVoronShouldWork2()
        {
			using (var storage = NewTransactionalStorage(requestedStorage: "voron", dataDir: path, tempDir: temp, runInMemory: false))
            {
                var scratchFile = Path.Combine(path, "scratch.buffers");
                var scratchFileTemp = Path.Combine(temp, "scratch.buffers");

                Assert.False(File.Exists(scratchFile));
                Assert.True(File.Exists(scratchFileTemp));
            }
        }
    }
}