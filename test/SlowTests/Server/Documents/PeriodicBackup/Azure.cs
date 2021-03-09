using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup
{
    public class Azure : NoDisposalNeeded
    {
        public Azure(ITestOutputHelper output) : base(output)
        {
        }

        internal const string AzureAccountName = "devstoreaccount1";
        internal const string AzureAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        [AzureStorageEmulatorFact]
        public void CanRemoveBlobsInBatch()
        {
            var containerName = Guid.NewGuid().ToString();
            var blobKey = Guid.NewGuid().ToString();

            using var client = new RavenAzureClient(GetAzureSettings(containerName));
            var blobs = new List<string>();

            try
            {
                client.DeleteContainer();
                client.PutContainer();

                const string perfix = nameof(CanRemoveBlobsInBatch);

                for (int i = 0; i < 10; i++)
                {
                    var key = $"{perfix}/{blobKey}/northwind_{i}.ravendump";
                    var tmpArr = new byte[3];
                    new Random().NextBytes(tmpArr);
                    client.PutBlob(key, new MemoryStream(tmpArr), new Dictionary<string, string> {{$"property_{i}", $"value_{i}"}});

                    var blob = client.GetBlob(key);
                    Assert.NotNull(blob);

                    blobs.Add(key);
                }

                client.DeleteBlobs(blobs);

                var listBlobs = client.ListBlobs(perfix, null, listFolders: false);
                var blobNames = listBlobs.List.Select(b => b.Name).ToList();
                Assert.Equal(0, blobNames.Count);
            }
            finally
            {
                client.DeleteContainer();
            }
        }

        [AzureStorageEmulatorFact]
        public void RemoveNonExistingBlobsInBatchShouldThrow()
        {
            var containerName = Guid.NewGuid().ToString();
            var blobKey = Guid.NewGuid().ToString();

            using var client = new RavenAzureClient(GetAzureSettings(containerName));
            var blobs = new List<string>();
            try
            {
                client.DeleteContainer();
                client.PutContainer();

                // put blob
                var k = $"{blobKey}/northwind_322.ravendump";
                var tmpArr = new byte[3];
                new Random().NextBytes(tmpArr);
                client.PutBlob(k, new MemoryStream(tmpArr), new Dictionary<string, string> {{"Nice", "NotNice"}});

                var blob = client.GetBlob(k);
                Assert.NotNull(blob);
                blobs.Add(k);

                for (int i = 0; i < 10; i++)
                {
                    blobs.Add($"{blobKey}/northwind_{i}.ravendump");
                }

                try
                {
                    client.DeleteBlobs(blobs);
                }
                catch (Exception e)
                {
                    Assert.Equal(typeof(InvalidOperationException), e.GetType());
                    Assert.True(e.Message.StartsWith(
                        $"Failed to delete {blobs.Count - 1} blobs from container: {containerName}. Successfully deleted 1 blob. Reason: The specified blob does not exist."));
                }
            }
            finally
            {
                client.DeleteContainer();
            }
        }

        [AzureStorageEmulatorFact]
        public void put_blob()
        {
            var containerName = Guid.NewGuid().ToString();
            var blobKey = Guid.NewGuid().ToString();

            using (var client = new RavenAzureClient(GetAzureSettings(containerName)))
            {
                try
                {
                    client.DeleteContainer();
                    client.PutContainer();

                    client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes("123")), new Dictionary<string, string>
                    {
                        {"property1", "value1"},
                        {"property2", "value2"}
                    });
                    var blob = client.GetBlob(blobKey);
                    Assert.NotNull(blob);

                    using (var reader = new StreamReader(blob.Data))
                        Assert.Equal("123", reader.ReadToEnd());

                    var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                    var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                    Assert.Equal("value1", blob.Metadata[property1]);
                    Assert.Equal("value2", blob.Metadata[property2]);
                }
                finally
                {
                    client.DeleteContainer();
                }
            }
        }

        [AzureStorageEmulatorFact]
        public void put_blob_in_folder()
        {
            var containerName = Guid.NewGuid().ToString();
            var blobKey = Guid.NewGuid() + "/" + Guid.NewGuid();

            using (var client = new RavenAzureClient(GetAzureSettings(containerName)))
            {
                try
                {
                    client.DeleteContainer();
                    client.PutContainer();

                    client.PutBlob(blobKey, new MemoryStream(Encoding.UTF8.GetBytes("123")),
                        new Dictionary<string, string> { { "property1", "value1" }, { "property2", "value2" } });

                    var blob = client.GetBlob(blobKey);
                    Assert.NotNull(blob);

                    using (var reader = new StreamReader(blob.Data))
                        Assert.Equal("123", reader.ReadToEnd());

                    var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                    var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                    Assert.Equal("value1", blob.Metadata[property1]);
                    Assert.Equal("value2", blob.Metadata[property2]);
                }
                finally
                {
                    client.DeleteContainer();
                }
            }
        }

        [AzureStorageEmulatorFact]
        public void put_blob_without_sas_token()
        {
            PutBlobs(5, false);
        }

        [AzureStorageEmulatorFact(Skip = "Azure Storage Emulator doesn't support SAS tokens")]
        public void put_blob_with_sas_token()
        {
            PutBlobs(5, true);
        }

        private static void PutBlobs(int blobsCount, bool useSasToken)
        {
            var containerName = Guid.NewGuid().ToString();
            var blobNamesToPut = new List<string>();
            for (var i = 0; i < blobsCount; i++)
            {
                blobNamesToPut.Add($"azure/{Guid.NewGuid()}/{i}");
            }

            var sasToken = useSasToken ? GetSasTokenAndCreateTheContainer(containerName) : null;
            using (var client = new RavenAzureClient(GetAzureSettings(containerName, sasToken)))
            {
                try
                {
                    if (useSasToken == false)
                    {
                        client.DeleteContainer();
                        client.PutContainer();
                    }

                    for (var i = 0; i < blobsCount; i++)
                    {
                        client.PutBlob(blobNamesToPut[i], new MemoryStream(Encoding.UTF8.GetBytes("123")),
                            new Dictionary<string, string> { { "property1", "value1" }, { "property2", "value2" } });
                    }

                    for (var i = 0; i < blobsCount; i++)
                    {
                        var blob = client.GetBlob(blobNamesToPut[i]);
                        Assert.NotNull(blob);

                        using (var reader = new StreamReader(blob.Data))
                            Assert.Equal("123", reader.ReadToEnd());

                        var property1 = blob.Metadata.Keys.Single(x => x.Contains("property1"));
                        var property2 = blob.Metadata.Keys.Single(x => x.Contains("property2"));

                        Assert.Equal("value1", blob.Metadata[property1]);
                        Assert.Equal("value2", blob.Metadata[property2]);
                    }

                    var listBlobs = client.ListBlobs("azure", null, listFolders: false);
                    var blobNames = listBlobs.List.Select(b => b.Name).ToList();
                    Assert.Equal(blobsCount, blobNames.Count);

                    // delete all blobs
                    client.DeleteBlobs(blobNames);

                    listBlobs = client.ListBlobs("azure", null, listFolders: false);
                    blobNames = listBlobs.List.Select(b => b.Name).ToList();
                    Assert.Equal(0, blobNames.Count);

                    for (var i = 0; i < blobsCount; i++)
                    {
                        var blob = client.GetBlob(blobNamesToPut[i]);
                        Assert.Null(blob);
                    }
                }
                finally
                {
                    client.DeleteContainer();
                }
            }
        }

        private static string GetSasTokenAndCreateTheContainer(string containerName)
        {
            var command =
                @$"$context = New-AzStorageContext -Local
New-AzStorageContainer {containerName} -Permission Off -Context $context
$now = Get-Date
New-AzStorageContainerSASToken -Name {containerName} -Permission rwdl -ExpiryTime $now.AddDays(1.0) -Context $context
";
            var startInfo = new ProcessStartInfo
            {
                FileName = "powershell.exe",
                Arguments = $"-c \"{command}\"",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            var process = new Process {StartInfo = startInfo};
            process.Start();

            while (true)
            {
                if (process.StandardOutput.EndOfStream)
                    break;

                var line = process.StandardOutput.ReadLine();
                if (line.StartsWith("?") == false)
                    continue;

                return line.Substring(1, line.Length - 2);
            }

            throw new InvalidOperationException($"Failed to get the SasToken from the emulator, error: {process.StandardError.ReadToEnd()}");
        }

        public static AzureSettings GetAzureSettings(string containerName, string sasToken = null)
        {
            return new AzureSettings
            {
                AccountName = AzureAccountName,
                AccountKey = sasToken == null ? AzureAccountKey : null,
                SasToken = sasToken,
                StorageContainer = containerName
            };
        }
    }
}
