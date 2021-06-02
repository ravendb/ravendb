// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4613.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class RavenDB_4163 : RavenTestBase
    {
        public RavenDB_4163(ITestOutputHelper output) : base(output)
        {
        }

        [AzureFact]
        public void put_blob_4MB_chunked()
        {
            PutBlob(sizeInMB: 4, testBlobKeyAsFolder: false, UploadType.Chunked);
        }

        [AzureFact]
        public void put_blob_into_folder_4MB_chunked()
        {
            PutBlob(sizeInMB: 4, testBlobKeyAsFolder: true, UploadType.Chunked);
        }

        // ReSharper disable once InconsistentNaming
        private void PutBlob(int sizeInMB, bool testBlobKeyAsFolder, UploadType uploadType)
        {
            var progress = new Progress();
            using (var holder = new Azure.AzureClientHolder(AzureFactAttribute.AzureSettings, progress))
            {
                holder.Client.MaxUploadPutBlobInBytes = 3 * 1024 * 1024;
                holder.Client.OnePutBlockSizeLimitInBytes = 1 * 1024 * 1024;

                var blobKey = testBlobKeyAsFolder == false
                    ? holder.Settings.RemoteFolderName
                    : $"{holder.Settings.RemoteFolderName}/folder/testKey";

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
                    holder.Client.PutBlob(blobKey, file,
                        new Dictionary<string, string>
                        {
                                    {"property1", value1},
                                    {"property2", value2},
                                    {"property3", value3}
                        });
                }

                var blob = holder.Client.GetBlob(blobKey);
                Assert.NotNull(blob);

                using (var reader = new StreamReader(blob.Data))
                {
                    var readBuffer = new char[buffer.Length];

                    long read, totalRead = 0;
                    while ((read = reader.Read(readBuffer, 0, readBuffer.Length)) > 0)
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
        }
    }
}
