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
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Attachments;

public abstract class RemoteAttachmentsS3Base : RemoteAttachmentsHolder<RemoteAttachmentsS3Settings>
{
    protected RemoteAttachmentsS3Base RemoteAttachments;
    protected RemoteAttachmentsS3Base(ITestOutputHelper output) : base(output)
    {
        RemoteAttachments = this;
    }

    public override IAsyncDisposable CreateCloudSettings([CallerMemberName] string caller = null)
    {
        Settings = Etl.GetS3Settings(nameof(RemoteAttachments), $"{caller}-{Guid.NewGuid()}").ToRemoteAttachmentsS3Settings();
        Assert.NotNull(Settings);

        return new AsyncDisposableAction(async () =>
        {
            await DisposeAttachmentsAndDeleteObjects();
        });
    }

    public override async Task<string> PutRemoteAttachmentsConfiguration(IDocumentStore store, RemoteAttachmentsS3Settings settings, List<string> collections = null, string database = null, string id = null)
    {
        if (string.IsNullOrEmpty(database))
            database = store.Database;

        id ??= "conf-identifier-s3";
        var config = new RemoteAttachmentsConfiguration()
        {
            Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
            {
                {
                    id, new RemoteAttachmentsDestinationConfiguration()
                    {
                        S3Settings = settings,
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

    public override RemoteAttachmentsS3Settings GetCloudSetting(string remoteFolderName, string caller = null)
    {
        var settings = Etl.GetS3Settings(remoteFolderName, caller).ToRemoteAttachmentsS3Settings();
        return settings;
    }

    protected override void AssertUploadRemoteAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal(RavenException e)
    {
        Assert.Contains("The specified key does not exist", e.Message);
    }

    protected override async Task<List<FileInfoDetails>> GetBlobsFromCloudAndAssertForCount(RemoteAttachmentsS3Settings settings, int expected, int timeout = 120_000)
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

    protected override async Task OverwriteBlobInCloudWithDummyStream(RemoteAttachmentsS3Settings settings, FileInfoDetails file)
    {
        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes("EGOR")))
        using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
        using (var s3Client = new RavenAwsS3Client(settings, EtlTestBase.DefaultBackupConfiguration, cancellationToken: cts.Token))
        {
            await s3Client.PutObjectAsync(file.FullPath, stream, new Dictionary<string, string>
            {
                { "Description", "GetBackupDescription" }
            });
        }
    }

    public override async Task DeleteObjects(RemoteAttachmentsS3Settings settings)
    {
        if (settings == null)
            return;

        await S3TestsHelper.DeleteObjects(settings, prefix: $"{settings.RemoteFolderName}", delimiter: string.Empty);
    }
}
