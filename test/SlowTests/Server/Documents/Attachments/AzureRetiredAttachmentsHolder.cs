using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Azure;
using SlowTests.Server.Documents.ETL.Olap;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments;

public abstract class AzureRetiredAttachmentsHolder : RetiredAttachmentsHolder<AzureSettings>
{
    protected AzureRetiredAttachmentsHolder RetiredAttachments;

    protected AzureRetiredAttachmentsHolder(ITestOutputHelper output) : base(output)
    {
        RetiredAttachments = this;
    }

    public override IAsyncDisposable CreateCloudSettings([CallerMemberName] string caller = null)
    {
        Settings = Etl.GetAzureSettings(nameof(RetiredAttachments), $"{caller}-{Guid.NewGuid()}");
        Assert.NotNull(Settings);

        return new AsyncDisposableAction(async () =>
        {
            await DisposeAttachmentsAndDeleteObjects();
        });
    }

    public override async Task PutRetireAttachmentsConfiguration(DocumentStore store, AzureSettings settings, List<string> collections = null, string database = null)
    {
        if (collections == null)
            collections = new List<string> { "Orders" };
        if (string.IsNullOrEmpty(database))
            database = store.Database;

        var config = new RetiredAttachmentsConfiguration()
        {
            AzureSettings = settings, Disabled = false, RetirePeriods = collections.ToDictionary(x => x, x => TimeSpan.FromMinutes(3)), RetireFrequencyInSec = 1000
        };
        ModifyRetiredAttachmentsConfig?.Invoke(config);
        await store.Maintenance.ForDatabase(database).SendAsync(new ConfigureRetiredAttachmentsOperation(config));
    }

    protected override async Task<List<FileInfoDetails>> GetBlobsFromCloudAndAssertForCount(AzureSettings settings, int expected, int timeout = 120_000)
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

    public override async Task DeleteObjects(AzureSettings AzureSettings)
    {
        if (AzureSettings == null)
            return;

        await AzureTests.DeleteObjects(AzureSettings, prefix: $"{AzureSettings.RemoteFolderName}", delimiter: string.Empty);
    }
}
