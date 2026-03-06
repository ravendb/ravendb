using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Commercial;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25696 : RavenTestBase
{
    public RavenDB_25696(ITestOutputHelper output) : base(output)
    {
    }

    [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
    public async Task ExceptionWhenImportingRemoteAttachmentsWithCommunityLicense()
    {
        await ExceptionWhenImportingRemoteAttachmentsWithUnsupportedLicense(LicenseTestBase.RL_COMM);
    }

    [RavenMultiLicenseRequiredFact(RavenTestCategory.Licensing | RavenTestCategory.Replication)]
    public async Task ExceptionWhenImportingRemoteAttachmentsWithProfessionalLicense()
    {
        await ExceptionWhenImportingRemoteAttachmentsWithUnsupportedLicense(LicenseTestBase.RL_PRO);
    }

    private async Task ExceptionWhenImportingRemoteAttachmentsWithUnsupportedLicense(string license)
    {
        DoNotReuseServer();

        var file = GetTempFileName();
        var dbName = $"db/{Guid.NewGuid()}";
        try
        {
            using (var store = GetDocumentStore())
            {
                await LicenseHelper.DisableRevisionCompression(Server, store);
                await LicenseHelper.CreateRemoteAttachmentsConfiguration(dbName, store);

                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            using (var store = GetDocumentStore())
            {
                await LicenseHelper.ChangeLicenseAndDisableRevisionCompression(Server, store, license);

                var exception = await Assert.ThrowsAsync<LicenseLimitException>(async () =>
                {
                    var importOperation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                    await importOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(5));
                });
                Assert.Equal(LimitType.RemoteAttachments, exception.LimitType);
                Assert.True(exception.Message.Contains("Your license doesn't support adding the remote attachments configuration."));
            }
        }
        finally
        {
            IOExtensions.DeleteFile(file);
        }
    }
}
