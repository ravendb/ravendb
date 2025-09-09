using System.Collections.Generic;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments
{
    public class AzureRetiredAttachmentsSlowTests : RetiredAttachmentsAzureBase
    {
        public AzureRetiredAttachmentsSlowTests(ITestOutputHelper output) : base(output)
        {
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToAzureAndGet(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentFromDifferentCollectionsToAzureAndGet(int attachmentsCount, int size)
        {
            var collections = new List<string> { "Orders", "Products" };
            Assert.True(attachmentsCount > 32, "this test meant to have more than 32 attachments so we will have more than one document");
            await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, collections: collections);
        }


        [AzureRetryTheory]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentFromDifferentCollectionsToAzureAndDelete(int attachmentsCount, int size)
        {
            Assert.True(attachmentsCount > 32, "this test meant to have more than 32 attachments so we will have more than one document");
            var collections = new List<string> { "Orders", "Products" };
            await CanUploadRetiredAttachmentToCloudAndDeleteInternal(attachmentsCount, size, collections);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToAzureAndDelete(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudAndDeleteInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRetiredAttachmentToAzureAndDeleteInTheSameTime(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRetiredAttachmentToCloudAndDeleteInTheSameTimeInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AzureRetryFact]
        public async Task ShouldAddRetireAtToAttachmentMetadataUsingAzureConfiguration()
        {
            await ShouldAddRetireAtToAttachmentMetadataInternal();
        }

        [AzureRetryFact]
        public async Task ShouldNotThrowUsingRegularAttachmentsApiOnRetiredAttachmentToAzure()
        {
            await ShouldNotThrowUsingRegularAttachmentsApiOnRetiredAttachmentInternal();
        }

        [AzureRetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRetiredAttachmentsFromDifferentCollectionsToAzureAndGetInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            var collections = new List<string> { "Orders", "Products" };
            await CanUploadRetiredAttachmentsToCloudAndGetInBulkInternal(attachmentsCount, size, attachmentsPerDoc, collections);
        }

        [AzureRetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRetiredAttachmentsToAzureAndGetInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRetiredAttachmentsToCloudAndGetInBulkInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AzureRetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        public async Task CanUploadRetiredAttachmentsToAzureAndDeleteInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRetiredAttachmentsToCloudAndDeleteInBulkInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AzureRetryFact]
        public async Task CanUploadRetiredAttachmentToAzureIfItAlreadyExists_ShouldNotOverwrite()
        {
            await CanUploadRetiredAttachmentToCloudIfItAlreadyExists_ShouldNotOverwriteInternal(overwriteWithDummy: false);
        }

        [AzureRetryFact]
        public async Task CanUploadRetiredAttachmentToAzureIfItAlreadyExists_ShouldOverwriteIfBroken()
        {
            await CanUploadRetiredAttachmentToCloudIfItAlreadyExists_ShouldNotOverwriteInternal(overwriteWithDummy: true);
        }

        [AzureRetryFact]
        public async Task UploadRetiredAttachmentToAzureThenManuallyDeleteAndGetShouldThrow()
        {
            await UploadRetiredAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal();
        }

        [AzureRetryFact]
        public async Task CanDeleteRetiredAttachmentFromAzureWhenItsNotExistsInAzure()
        {
            await CanDeleteRetiredAttachmentFromCloudWhenItsNotExistsInCloudInternal();
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToAzureInClusterAndGet(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudInClusterAndGetInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToAzureInClusterAndGet2(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudInClusterAndGet2Internal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToAzureInClusterAndDelete(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudInClusterAndDeleteInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToAzureFromBackupAndGet(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudFromBackupAndGet(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExternalReplicateRetiredAttachmentAndThenUploadToAzureAndGet(int attachmentsCount, int size)
        {
            await CanExternalReplicateRetiredAttachmentAndThenUploadToCloudAndGet(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        public async Task CanBackupRetiredAttachments(int attachmentsCount, int size)
        {
            await CanBackupRetiredAttachmentsInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExportImportWithRetiredAttachment(int attachmentsCount, int size)
        {
            await CanExportImportWithRetiredAttachmentInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanIndexWithRetiredAttachment(int attachmentsCount, int size)
        {
            await CanIndexWithRetiredAttachmentInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanEtlWithRetiredAttachmentAndRetireOnDestination(int attachmentsCount, int size)
        {
            await CanEtlWithRetiredAttachmentAndRetireOnDestinationInternal(attachmentsCount, size);
        }

        [AzureRetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanEtlRetiredAttachmentsToDestination(int attachmentsCount, int size)
        {
            await CanEtlRetiredAttachmentsToDestinationInternal(attachmentsCount, size);
        }
    }
}
