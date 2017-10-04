// -----------------------------------------------------------------------
//  <copyright file="RavenDB_7412.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Extensions;
using Raven.Tests.Common;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_7412 : RavenFilesTestBase
    {
        private const string FileName = "file.txt";

        [Fact]
        public async Task InMasterMasterSetupCanSynchronizeBackTheDelete()
        {
            await Test(secondary => secondary.DeleteAsync(FileName));
        }

        [Fact]
        public async Task InMasterMasterSetupCanSynchronizeBackTheDeleteAfterDeletingByQuery()
        {
            await Test(secondary => secondary.DeleteByQueryAsync("__directory:/"));
        }

        private async Task Test(Func<IAsyncFilesCommands, Task> deleteOnSecondary)
        {
            var c1 = NewAsyncClient(0);
            var c2 = NewAsyncClient(1);

            await c1.Synchronization.SetDestinationsAsync(c2.ToSynchronizationDestination());
            await c2.Synchronization.SetDestinationsAsync(c1.ToSynchronizationDestination());

            await c1.UploadAsync(FileName, CreateRandomFileStream(10));

            WaitForFile(c2, FileName);

            Assert.NotNull(await c2.DownloadAsync(FileName));

            await deleteOnSecondary(c2);

            await AssertAsync.Throws<FileNotFoundException>(() => c2.DownloadAsync(FileName));

            WaitForFileDelete(c1, FileName);
        }

        private new static void WaitForFile(IAsyncFilesCommands client, string fileName)
        {
            var result = SpinWait.SpinUntil(() =>
            {
                try
                {
                    var file = AsyncHelpers.RunSync(() => client.DownloadAsync(fileName));
                    return file != null;
                }
                catch (FileNotFoundException)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(30));

            if (result)
                return;

            throw new TimeoutException("Could not download file " + fileName);
        }

        private static void WaitForFileDelete(IAsyncFilesCommands client, string fileName)
        {
            var result = SpinWait.SpinUntil(() =>
            {
                try
                {
                    var file = AsyncHelpers.RunSync(() => client.DownloadAsync(fileName));
                    return file == null;
                }
                catch (FileNotFoundException)
                {
                    return true;
                }
            }, TimeSpan.FromSeconds(30));

            if (result)
                return;

            throw new TimeoutException("Could retrieve file " + fileName + " while it was supposed to be deleted");
        }
    }
}