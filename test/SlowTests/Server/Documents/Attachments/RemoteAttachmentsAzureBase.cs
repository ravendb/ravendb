using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments.Remote;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Azure;
using SlowTests.Server.Documents.ETL.Olap;
using Xunit;

namespace SlowTests.Server.Documents.Attachments;

public abstract class RemoteAttachmentsAzureBase : RemoteAttachmentsHolder<RemoteAttachmentsAzureSettings>
{
    protected RemoteAttachmentsAzureBase RemoteAttachments;

    protected RemoteAttachmentsAzureBase(ITestOutputHelper output) : base(output)
    {
        RemoteAttachments = this;
    }

    public override IAsyncDisposable CreateCloudSettings([CallerMemberName] string caller = null)
    {
        Settings = Etl.GetAzureSettings(nameof(RemoteAttachments), $"{caller}-{Guid.NewGuid()}").ToRemoteAttachmentsAzureSettings();
        Assert.NotNull(Settings);

        return new AsyncDisposableAction(async () =>
        {
            await DisposeAttachmentsAndDeleteObjects();
        });
    }

    public override async Task<string> PutRemoteAttachmentsConfiguration(IDocumentStore store, RemoteAttachmentsAzureSettings settings, List<string> collections = null, string database = null, string id = null)
    {
        collections ??= ["Orders"];

        if (string.IsNullOrEmpty(database))
            database = store.Database;
        
        id ??= "conf-identifier-azure";
        var config = new RemoteAttachmentsConfiguration
        {
            Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>
            {
                {
                    id, new RemoteAttachmentsDestinationConfiguration
                    {
                        AzureSettings = settings,
                        Disabled = false,
                    }
                }
            },
            CheckFrequencyInSec = 1000
        };

        ModifyRemoteAttachmentsConfig?.Invoke(config);
        await store.Maintenance.ForDatabase(database).SendAsync(new ConfigureRemoteAttachmentsOperation(config));

        return id;
    }

    protected override async Task<List<FileInfoDetails>> GetBlobsFromCloudAndAssertForCount(RemoteAttachmentsAzureSettings settings, int expected, int timeout = 120_000)
    {
        List<RavenStorageClient.BlobProperties> cloudObjects = null;
        var val3 = await WaitForValueAsync(async () =>
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var client = RavenAzureClient.Create(settings, EtlTestBase.DefaultBackupConfiguration, cancellationToken: cts.Token))
            {
                var prefix = $"{settings.RemoteFolderName}";
                cloudObjects = (await client.ListBlobsAsync(prefix, delimiter: string.Empty, listFolders: false)).List.ToList();
                return cloudObjects.Count;
            }
        }, expected, timeout);
        Assert.Equal(expected, val3);

        if (expected == 0)
            Assert.Empty(cloudObjects);
        else
            Assert.NotNull(cloudObjects);

        return cloudObjects.Select(x => new FileInfoDetails()
        {
            FullPath = x.Name,
            LastModified = x.LastModified?.DateTime ?? DateTime.MinValue
        }).ToList();
    }

    protected override Task OverwriteBlobInCloudWithDummyStream(RemoteAttachmentsAzureSettings settings, FileInfoDetails file)
    {
        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes("EGOR")))
        using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
        using (var client = RavenAzureClient.Create(settings, EtlTestBase.DefaultBackupConfiguration, cancellationToken: cts.Token))
        {
            client.PutBlob(file.FullPath, stream, new Dictionary<string, string>
            {
                { "Description", "GetBackupDescription" }
            });
        }
        return Task.CompletedTask;
    }

    public override async Task DeleteObjects(RemoteAttachmentsAzureSettings settings)
    {
        if (settings == null)
            return;

        await AzureTests.DeleteObjects(settings, prefix: $"{settings.RemoteFolderName}", delimiter: string.Empty);
    }

    public override RemoteAttachmentsAzureSettings GetCloudSetting(string remoteFolderName, string caller = null)
    {
        var settings = Etl.GetAzureSettings(remoteFolderName, caller).ToRemoteAttachmentsAzureSettings();
        return settings;
    }

    protected override async Task WaitForTaskDelayIfNeeded()
    {
        await Task.Delay(1000); // in Azure we have seconds resolution
    }

    protected override void AssertUploadRemoteAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal(RavenException e)
    {
        Assert.Contains("The specified blob does not exist.", e.Message);
    }
}
