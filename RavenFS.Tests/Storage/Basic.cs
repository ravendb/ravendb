// -----------------------------------------------------------------------
//  <copyright file="Basic.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

using Xunit;
using Xunit.Extensions;

namespace RavenFS.Tests.Storage
{
    public class Basic : StorageAccessorTestBase
    {
        [Fact]
        public void InMemory()
        {
            var path = NewDataPath();

            if (Directory.Exists(path))
                Directory.Delete(path, true);

            using (NewTransactionalStorage("voron", path: path))
            {
                Assert.False(Directory.Exists(path));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void File(string requestedStorage)
        {
            var path = NewDataPath();

            if (Directory.Exists(path))
                Directory.Delete(path, true);

            using (NewTransactionalStorage(requestedStorage: requestedStorage, path: path, runInMemory: false))
            {
                Assert.True(Directory.Exists(path));
            }
        }
    }
}