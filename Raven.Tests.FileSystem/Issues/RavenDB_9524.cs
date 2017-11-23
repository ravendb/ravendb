// -----------------------------------------------------------------------
//  <copyright file="RavenDB_9524.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_9524: RavenFilesTestWithLogs
    {
        [Theory]
        [PropertyData("Storages")]
        public async Task Can_change_internal_fs_id(string storage)
        {
            var client = NewAsyncClient(requestedStorage: storage);
            var rfs = GetFileSystem();

            var stats = await client.GetStatisticsAsync();

            rfs.Storage.ChangeId();

            var newStats = await client.GetStatisticsAsync();

            Assert.NotEqual(stats.FileSystemId, newStats.FileSystemId);
        }
    }
}