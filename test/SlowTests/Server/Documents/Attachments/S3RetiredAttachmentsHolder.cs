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
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Restore;
using SlowTests.Server.Documents.ETL.Olap;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments;

public class S3RetiredAttachmentsHolder : RetiredAttachmentsHolder<S3Settings>
{
    protected S3RetiredAttachmentsHolder RetiredAttachments;
    public S3RetiredAttachmentsHolder(ITestOutputHelper output) : base(output)
    {
        RetiredAttachments = this;
    }

    public override IAsyncDisposable CreateCloudSettings([CallerMemberName] string caller = null)
    {
        Settings = Etl.GetS3Settings(nameof(RetiredAttachments), $"{caller}-{Guid.NewGuid()}");
        Assert.NotNull(Settings);

        return new AsyncDisposableAction(async () =>
        {
            await DisposeAttachmentsAndDeleteObjects();
        });
    }

    public override async Task PutRetireAttachmentsConfiguration(DocumentStore store, S3Settings settings, List<string> collections = null, string database = null)
    {
        if (collections == null)
            collections = new List<string> { "Orders" };
        if (string.IsNullOrEmpty(database))
            database = store.Database;

        var config = new RetiredAttachmentsConfiguration()
        {
            S3Settings = settings, Disabled = false, RetirePeriods = collections.ToDictionary(x => x, x => TimeSpan.FromMinutes(3)), RetireFrequencyInSec = 1000
        };
        ModifyRetiredAttachmentsConfig?.Invoke(config);
        await store.Maintenance.ForDatabase(database).SendAsync(new ConfigureRetiredAttachmentsOperation(config));
    }

    protected override void AssertUploadRetiredAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal(RavenException e)
    {
        Assert.Contains("The specified key does not exist", e.Message);
    }

    protected override async Task<List<FileInfoDetails>> GetBlobsFromCloudAndAssertForCount(S3Settings settings, int expected, int timeout = 120_000)
    {
        List<S3FileInfoDetails> cloudObjects = null;
        var val3 = await WaitForValueAsync(async () =>
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var s3Client = new RavenAwsS3Client(settings, EtlTestBase.DefaultBackupConfiguration, cancellationToken: cts.Token))
            {
                var prefix = $"{settings.RemoteFolderName}";
                cloudObjects = await s3Client.ListAllObjectsAsync(prefix, string.Empty, false);
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
            FullPath = x.FullPath,
            LastModified = x.LastModified
        }).ToList();
    }

    public override async Task DeleteObjects(S3Settings s3Settings)
    {
        if (s3Settings == null)
            return;

        await S3Tests.DeleteObjects(s3Settings, prefix: $"{s3Settings.RemoteFolderName}", delimiter: string.Empty);
    }
}
