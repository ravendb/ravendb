using System;
using System.Diagnostics;
using FastTests.Voron.Optimizations;
using Microsoft.CodeAnalysis;
using Tests.Infrastructure;
using Voron;
using Voron.Exceptions;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron
{
    public class StorageEnvironmentTests : StorageTest
    {
        public StorageEnvironmentTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanSetDatabaseId()
        {
            var previousDatabaseId = Env.DbId;
            var guid = Guid.NewGuid();

            Env.SetDatabaseId(guid.ToString());
            Assert.NotEqual(previousDatabaseId, Env.DbId);

            using (var readTx = Env.ReadTransaction())
            {
                var metadataTree = readTx.ReadTree(Constants.MetadataTreeNameSlice);
                var dbId = metadataTree.Read(Constants.MetadataDbId);
                var buffer = new byte[16];
                dbId.Reader.Read(buffer, 0, 16);
                Assert.Equal(guid, new Guid(buffer));
            }
        }
    }
}
