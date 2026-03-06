using System.Collections.Generic;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Attachments
{
    public class AzureRemoteAttachmentsSlowTests : RemoteAttachmentsAzureBase
    {
        public AzureRemoteAttachmentsSlowTests(ITestOutputHelper output) : base(output)
        {
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentToAzureAndGet(int attachmentsCount, int size)
        {
            await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentFromDifferentCollectionsToAzureAndGet(int attachmentsCount, int size)
        {
            var collections = new List<string> { "Orders", "Products" };
            Assert.True(attachmentsCount > 32, "this test meant to have more than 32 attachments so we will have more than one document");
            await CanUploadRemoteAttachmentToCloudAndGetInternal(attachmentsCount, size, collections: collections);
        }


        [AzureRetryTheory]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentFromDifferentCollectionsToAzureAndDelete(int attachmentsCount, int size)
        {
            Assert.True(attachmentsCount > 32, "this test meant to have more than 32 attachments so we will have more than one document");
            var collections = new List<string> { "Orders", "Products" };
            await CanUploadRemoteAttachmentToCloudAndDeleteInternal(attachmentsCount, size, collections);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentToAzureAndDelete(int attachmentsCount, int size)
        {
            await CanUploadRemoteAttachmentToCloudAndDeleteInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRemoteAttachmentToAzureAndDeleteInTheSameTime(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRemoteAttachmentToCloudAndDeleteInTheSameTimeInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AzureRetryFact]
        public async Task ShouldAddRemoteAtToAttachmentMetadataUsingAzureConfiguration()
        {
            await ShouldAddRemoteAtToAttachmentMetadataInternal();
        }

        [AzureRetryFact]
        public async Task ShouldNotThrowUsingRegularAttachmentsApiOnRemoteAttachmentToAzure()
        {
            await ShouldNotThrowUsingRegularAttachmentsApiOnRemoteAttachmentInternal();
        }

        [AzureRetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRemoteAttachmentsFromDifferentCollectionsToAzureAndGetInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            var collections = new List<string> { "Orders", "Products" };
            await CanUploadRemoteAttachmentsToCloudAndGetInBulkInternal(attachmentsCount, size, attachmentsPerDoc, collections);
        }

        [AzureRetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRemoteAttachmentsToAzureAndGetInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRemoteAttachmentsToCloudAndGetInBulkInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AzureRetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRemoteAttachmentsToAzureAndDeleteInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRemoteAttachmentsToCloudAndDeleteInBulkInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AzureRetryFact]
        public async Task CanUploadRemoteAttachmentToAzureIfItAlreadyExists_ShouldNotOverwrite()
        {
            await CanUploadRemoteAttachmentToCloudIfItAlreadyExists_ShouldNotOverwriteInternal(overwriteWithDummy: false);
        }

        [AzureRetryFact]
        public async Task CanUploadRemoteAttachmentToAzureIfItAlreadyExists_ShouldOverwriteIfBroken()
        {
            await CanUploadRemoteAttachmentToCloudIfItAlreadyExists_ShouldNotOverwriteInternal(overwriteWithDummy: true);
        }

        [AzureRetryFact]
        public async Task UploadRemoteAttachmentToAzureThenManuallyDeleteAndGetShouldThrow()
        {
            await UploadRemoteAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal();
        }

        [AzureRetryFact]
        public async Task CanDeleteRemoteAttachmentFromAzureWhenItsNotExistsInAzure()
        {
            await CanDeleteRemoteAttachmentFromCloudWhenItsNotExistsInCloudInternal();
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentToAzureInClusterAndGet(int attachmentsCount, int size)
        {
            await CanUploadRemoteAttachmentToCloudInClusterAndGetInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentToAzureInClusterAndGet2(int attachmentsCount, int size)
        {
            await CanUploadRemoteAttachmentToCloudInClusterAndGet2Internal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentToAzureInClusterAndDelete(int attachmentsCount, int size)
        {
            await CanUploadRemoteAttachmentToCloudInClusterAndDeleteInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRemoteAttachmentToAzureFromBackupAndGet(int attachmentsCount, int size)
        {
            await CanUploadRemoteAttachmentToCloudFromBackupAndGet(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExternalReplicateRemoteAttachmentAndThenUploadToAzureAndGet(int attachmentsCount, int size)
        {
            await CanExternalReplicateRemoteAttachmentAndThenUploadToCloudAndGet(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        public async Task CanBackupRemoteAttachments(int attachmentsCount, int size)
        {
            await CanBackupRemoteAttachmentsInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExportImportWithRemoteAttachment(int attachmentsCount, int size)
        {
            await CanExportImportWithRemoteAttachmentInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanIndexWithRemoteAttachment(int attachmentsCount, int size)
        {
            await CanIndexWithRemoteAttachmentInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanEtlWithRemoteAttachmentAndRemoteOnDestination(int attachmentsCount, int size)
        {
            await CanEtlWithRemoteAttachmentAndRemoteOnDestinationInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanEtlRemoteAttachmentsToDestination(int attachmentsCount, int size)
        {
            await CanEtlRemoteAttachmentsToDestinationInternal(attachmentsCount, size);
        }
    }
}
