// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4613.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Sparrow;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class RavenDB_4163 : RavenTestBase
    {
        private const string AzureAccountName = "devstoreaccount1";
        private const string AzureAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        [AzureStorageEmulatorFact]
        public async Task put_blob_64MB()
        {
            await PutBlob(64, false, UploadType.Regular);
        }

        [AzureStorageEmulatorFact]
        public async Task put_blob_70MB()
        {
            await PutBlob(70, false, UploadType.Regular);
        }

        [AzureStorageEmulatorFact]
        public async Task put_blob_100MB()
        {
            await PutBlob(100, false, UploadType.Regular);
        }

        [AzureStorageEmulatorFact]
        public async Task put_blob_256MB()
        {
            await PutBlob(256, false, UploadType.Regular);
        }

        [AzureStorageEmulatorFact]
        public async Task put_blob_500MB()
        {
            await PutBlob(500, false, UploadType.Chunked);
        }

        [AzureStorageEmulatorFact]
        public async Task put_blob_765MB()
        {
            await PutBlob(765, false, UploadType.Chunked);
        }

        [AzureStorageEmulatorFact]
        public async Task put_blob_into_folder_64MB()
        {
            await PutBlob(64, true, UploadType.Regular);
        }

        [AzureStorageEmulatorFact]
        public async Task put_blob_into_folder_70MB()
        {
            await PutBlob(70, true, UploadType.Regular);
        }

        [AzureStorageEmulatorFact]
        public async Task put_blob_into_folder_100MB()
        {
            await PutBlob(100, true, UploadType.Regular);
        }

        [AzureStorageEmulatorFact]
        public async Task put_blob_into_folder_256MB()
        {
            await PutBlob(256, true, UploadType.Regular);
        }

        [AzureStorageEmulatorFact]
        public async Task put_blob_into_folder_500MB()
        {
            await PutBlob(500, true, UploadType.Chunked);
        }

        [AzureStorageEmulatorFact]
        public async Task put_blob_into_folder_765MB()
        {
            await PutBlob(765, true, UploadType.Chunked);
        }

        [AzureStorageEmulatorFact]
        public async Task can_get_and_delete_container()
        {
            var containerName = Guid.NewGuid().ToString();
            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, isTest: true))
            {
                var containerNames = await client.GetContainerNames(500);
                Assert.False(containerNames.Exists(x => x.Equals(containerName)));
                await client.PutContainer();

                containerNames = await client.GetContainerNames(500);
                Assert.True(containerNames.Exists(x => x.Equals(containerName)));

                await client.DeleteContainer();

                containerNames = await client.GetContainerNames(500);
                Assert.False(containerNames.Exists(x => x.Equals(containerName)));
            }
        }

        [AzureStorageEmulatorFact]
        public async Task can_get_container_not_found()
        {
            var containerName = Guid.NewGuid().ToString();
            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, isTest: true))
            {
                var containerNames = await client.GetContainerNames(500);
                Assert.False(containerNames.Exists(x => x.Equals(containerName)));

                var e = await Assert.ThrowsAsync<ContainerNotFoundException>(async () => await client.TestConnection());
                Assert.Equal($"Container '{containerName}' not found!", e.Message);

                containerNames = await client.GetContainerNames(500);
                Assert.False(containerNames.Exists(x => x.Equals(containerName)));
            }
        }

        // ReSharper disable once InconsistentNaming
        private async Task PutBlob(int sizeInMB, bool testBlobKeyAsFolder, UploadType uploadType)
        {
            var containerName = Guid.NewGuid().ToString();
            var blobKey = testBlobKeyAsFolder == false ?
                Guid.NewGuid().ToString() :
                $"{Guid.NewGuid()}/folder/testKey";

            var progress = new Progress();
            using (var client = new RavenAzureClient(AzureAccountName, AzureAccountKey, containerName, progress, isTest: true))
            {
                try
                {
                    await client.DeleteContainer();
                    await client.PutContainer();

                    var path = NewDataPath(forceCreateDir: true);
                    var filePath = Path.Combine(path, Guid.NewGuid().ToString());

                    var sizeMb = new Size(sizeInMB, SizeUnit.Megabytes);
                    var size64Kb = new Size(64, SizeUnit.Kilobytes);

                    var buffer = Enumerable.Range(0, (int)size64Kb.GetValue(SizeUnit.Bytes))
                        .Select(x => (byte)'a')
                        .ToArray();

                    using (var file = File.Open(filePath, FileMode.CreateNew))
                    {
                        for (var i = 0; i < sizeMb.GetValue(SizeUnit.Bytes) / buffer.Length; i++)
                        {
                            file.Write(buffer, 0, buffer.Length);
                        }
                    }

                    var value1 = Guid.NewGuid().ToString();
                    var value2 = Guid.NewGuid().ToString();
                    var value3 = Guid.NewGuid().ToString();

                    long streamLength;
                    using (var file = File.Open(filePath, FileMode.Open))
                    {
                        streamLength = file.Length;
                        await client.PutBlob(blobKey, file,
                            new Dictionary<string, string>
                            {
                                    {"property1", value1},
                                    {"property2", value2},
                                    {"property3", value3}
                            });
                    }

                    var blob = await client.GetBlob(blobKey);
                    Assert.NotNull(blob);

                    using (var reader = new StreamReader(blob.Data))
                    {
                        var readBuffer = new char[buffer.Length];

                        long read, totalRead = 0;
                        while ((read = await reader.ReadAsync(readBuffer, 0, readBuffer.Length)) > 0)
                        {
                            for (var i = 0; i < read; i++)
                                Assert.Equal(buffer[i], (byte)readBuffer[i]);

                            totalRead += read;
                        }

                        Assert.Equal(streamLength, totalRead);
                    }

                    var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                    var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));
                    var property3 = blob.Metadata.Keys.Single(x => x.Contains("property3"));

                    Assert.Equal(value1, blob.Metadata[property1]);
                    Assert.Equal(value2, blob.Metadata[property2]);
                    Assert.Equal(value3, blob.Metadata[property3]);

                    Assert.Equal(UploadState.Done, progress.UploadProgress.UploadState);
                    Assert.Equal(uploadType, progress.UploadProgress.UploadType);
                    Assert.Equal(streamLength, progress.UploadProgress.TotalInBytes);
                    Assert.Equal(streamLength, progress.UploadProgress.UploadedInBytes);
                }
                finally
                {
                    await client.DeleteContainer();
                }
            }
        }
    }
}
