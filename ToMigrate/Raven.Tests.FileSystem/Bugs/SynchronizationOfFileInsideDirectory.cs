// -----------------------------------------------------------------------
//  <copyright file="SynchronizationOfFileInsideDirectory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.FileSystem;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Bugs
{
    public class SynchronizationOfFileInsideDirectory : RavenFilesTestWithLogs
    {
        [Theory]
        [InlineData("m s.bin")]
        [InlineData("/content/ms.bin")]
        [InlineData("/content/m s.bin")]
        public async Task SignaturesShouldWorkIfFileNameContainsFolders(string fileName)
        {
            var source = NewAsyncClient(0);
            var destination = NewAsyncClient(1);

            var r = new Random();

            var bytes = new byte[2*1024*1024];

            r.NextBytes(bytes);

            await source.UploadAsync(fileName, new MemoryStream(bytes));

            var syncResult = await source.Synchronization.StartAsync(fileName, destination);

            Assert.Null(syncResult.Exception);
            Assert.Equal(SynchronizationType.ContentUpdate, syncResult.Type);

            bytes = new byte[3 * 1024 * 1024];

            r.NextBytes(bytes);

            await source.UploadAsync(fileName, new MemoryStream(bytes));

            syncResult = await source.Synchronization.StartAsync(fileName, destination);

            Assert.Null(syncResult.Exception);
            Assert.Equal(SynchronizationType.ContentUpdate, syncResult.Type);
        }
    }
}