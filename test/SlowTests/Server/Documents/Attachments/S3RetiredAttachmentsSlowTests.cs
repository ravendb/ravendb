using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client;
using Orders;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments
{
    public  class S3RetiredAttachmentsSlowTests : S3RetiredAttachmentsHolder
    {
        //TODO: egor test CanUploadRetiredAttachmentToS3IfItAlreadyExists - will rewrite the retired attachment, even if it is the same - is it the behaviour we want?
        //TODO: egor do big attachments tests
        //TODO: egor test for "now we delete doc with retired attachemnt, it will delete the retire attachment from cloud!"
        public S3RetiredAttachmentsSlowTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task DeletingAttachmentShouldRemoveFromRetireTree()
        {
            await using (var holder = CreateCloudSettings())
            {
                int docsCount = GetDocsAndAttachmentCount(1, out int attachmentsPerDoc);
                var ids = new List<(string Id, string Collection)>();
                List<string> collections = null;

                using (var store = GetDocumentStore())
                {
                    await PutRetireAttachmentsConfiguration(store, Settings, collections);
                    await CreateDocs(store, docsCount, ids, collections);
                    await PopulateDocsWithRandomAttachments(store, 3, ids, attachmentsPerDoc);

                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    GetStorageAttachmentsMetadataFromAllAttachments(database);
                    Assert.Equal(1, Attachments.Count);

                    GetToRetireAttachmentsCount(database, 1);
                    var attachment = Attachments[0];
                    await store.Operations.SendAsync(new DeleteAttachmentOperation(attachment.DocumentId, attachment.Name));
                    GetToRetireAttachmentsCount(database, 0);
                }
            }
        }

        private static void GetToRetireAttachmentsCount(DocumentDatabase database, int expected)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                DatabaseRecord dbRecord;
                string nodeTag;

                using (database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                using (serverContext.OpenReadTransaction())
                {
                    dbRecord = database.ServerStore.Cluster.ReadDatabase(serverContext, database.Name);
                    nodeTag = database.ServerStore.NodeTag;
                }

                var options = new BackgroundWorkParameters(context, DateTime.MaxValue, dbRecord, nodeTag, int.MinValue);
                var totalCount = 0;

                using (database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.Initialize(context))
                {
                    var expired = database.DocumentsStorage.AttachmentsStorage.RetiredAttachmentsStorage.GetDocuments(options, ref totalCount, out _,
                        CancellationToken.None);
                    Assert.Equal(expected, totalCount);
                    if (expected == 0)
                    {
                        Assert.Null(expired);
                    }
                    else
                    {
                        Assert.Equal(expected, expired.Count);
                    }
                }
            }
        }


        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]
        //TODO: egor in the future need optimizations so I can do a lot faster[InlineData(256)]
        //[InlineData(1024)]
        public async Task CanUploadRetiredAttachmentToS3AndGet(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]
        //TODO: egor in the future need optimizations so I can do a lot faster[InlineData(256)]
        //[InlineData(1024, 3)]
        public async Task CanUploadRetiredAttachmentFromDifferentCollectionsToS3AndGet(int attachmentsCount, int size)
        {
            var collections = new List<string> { "Orders", "Products" };
            Assert.True(attachmentsCount > 32, "this test meant to have more than 32 attachments so we will have more than one document");
            await CanUploadRetiredAttachmentToCloudAndGetInternal(attachmentsCount, size, collections);
        }

        [AmazonS3RetryTheory]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentFromDifferentCollectionsToS3AndDelete(int attachmentsCount, int size)
        {
            Assert.True(attachmentsCount > 32, "this test meant to have more than 32 attachments so we will have more than one document");
            var collections = new List<string> { "Orders", "Products" };
            await CanUploadRetiredAttachmentToCloudAndDeleteInternal(attachmentsCount, size, collections);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3, true)]
        [InlineData(64, 3, true)]
        [InlineData(1, 3, false)]
        [InlineData(64, 3, false)]
        public async Task CanUploadRetiredAttachmentToS3AndDelete(int attachmentsCount, int size, bool storageOnly)
        {
            await CanUploadRetiredAttachmentToCloudAndDeleteInternal(attachmentsCount, size, storageOnly: storageOnly);
        }

        [AmazonS3RetryTheory]
        [InlineData(16, 3, 4)]
        //[InlineData(64, 3, 4)]
        public async Task CanUploadRetiredAttachmentToS3AndDeleteInTheSameTime(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRetiredAttachmentToCloudAndDeleteInTheSameTimeInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AmazonS3RetryFact]
        public async Task ShouldAddRetireAtToAttachmentMetadataUsingS3Configuration()
        {
            await ShouldAddRetireAtToAttachmentMetadataInternal();
        }

        [AmazonS3RetryFact]
        public async Task ShouldThrowUsingRegularAttachmentsApiOnRetiredAttachmentToS3()
        {
            await ShouldThrowUsingRegularAttachmentsApiOnRetiredAttachmentInternal();
        }


        [AmazonS3RetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        //[InlineData(128, 3)]
        public async Task CanUploadRetiredAttachmentsFromDifferentCollectionsToS3AndGetInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            var collections = new List<string> { "Orders", "Products" };
            await CanUploadRetiredAttachmentsToCloudAndGetInBulkInternal(attachmentsCount, size, attachmentsPerDoc, collections);
        }

        [AmazonS3RetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        //[InlineData(128, 3)]
        public async Task CanUploadRetiredAttachmentsToS3AndGetInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRetiredAttachmentsToCloudAndGetInBulkInternal(attachmentsCount, size, attachmentsPerDoc);
        }
        [AmazonS3RetryTheory]
        [InlineData(3, 3, 1)]
        [InlineData(16, 3, 4)]
        //[InlineData(128, 3)]
        public async Task CanUploadRetiredAttachmentsToS3AndDeleteInBulk(int attachmentsCount, int size, int attachmentsPerDoc)
        {
            await CanUploadRetiredAttachmentsToCloudAndDeleteInBulkInternal(attachmentsCount, size, attachmentsPerDoc);
        }

        [AmazonS3RetryFact]
        public async Task CanUploadRetiredAttachmentToS3IfItAlreadyExists()
        {
            await CanUploadRetiredAttachmentToCloudIfItAlreadyExistsInternal();
        }

        [AmazonS3RetryFact]
        public async Task UploadRetiredAttachmentToS3ThenManuallyDeleteAndGetShouldThrow()
        {
            await UploadRetiredAttachmentToCloudThenManuallyDeleteAndGetShouldThrowInternal();
        }

        [AmazonS3RetryFact]
        public async Task CanDeleteRetiredAttachmentFromS3WhenItsNotExistsInS3()
        {
            await CanDeleteRetiredAttachmentFromCloudWhenItsNotExistsInCloudInternal();
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToS3InClusterAndGet(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudInClusterAndGetInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToS3InClusterAndGet2(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudInClusterAndGet2Internal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanUploadRetiredAttachmentToS3InClusterAndDelete(int attachmentsCount, int size)
        {
            await CanUploadRetiredAttachmentToCloudInClusterAndDeleteInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
         [InlineData(64, 3)]
        //[InlineData(128, 3)]

        //TODO: egor add test that backup & restore already retired attachment (so the stream is null) (maybe should throw if there is no config?)
        public async Task CanUploadRetiredAttachmentToS3FromBackupAndGet(int attachmentsCount, int size)
        {

            await CanUploadRetiredAttachmentToCloudFromBackupAndGet(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        public async Task CanExternalReplicateRetiredAttachmentAndThenUploadToS3AndGet(int attachmentsCount, int size)
        {
            await CanExternalReplicateRetiredAttachmentAndThenUploadToCloudAndGet(attachmentsCount, size);
        }


        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        //[InlineData(64, 3)]
        //[InlineData(128, 3)]

        //TODO: egor add test that backup & restore already retired attachment (so the stream is null) (maybe should throw if there is no config?)
        public async Task CanBackupRetiredAttachments(int attachmentsCount, int size)
        {
            await CanBackupRetiredAttachmentsInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]

        //TODO: egor add test that backup & restore already retired attachment (so the stream is null) (maybe should throw if there is no config?)
        public async Task CanExportImportWithRetiredAttachment(int attachmentsCount, int size)
        {
            await CanExportImportWithRetiredAttachmentInternal(attachmentsCount, size);
        }


        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]

        //TODO: egor add test that backup & restore already retired attachment (so the stream is null) (maybe should throw if there is no config?)
        public async Task CanIndexWithRetiredAttachment(int attachmentsCount, int size)
        {
            await CanIndexWithRetiredAttachmentInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]

        public async Task CanEtlWithRetiredAttachmentAndRetireOnDestination(int attachmentsCount, int size)
        {
            await CanEtlWithRetiredAttachmentAndRetireOnDestinationInternal(attachmentsCount, size);
        }

        [AmazonS3RetryTheory]
        [InlineData(1, 3)]
        [InlineData(64, 3)]
        //[InlineData(128, 3)]

        public async Task CanEtlRetiredAttachmentsToDestination(int attachmentsCount, int size)
        {
            await CanEtlRetiredAttachmentsToDestinationInternal(attachmentsCount, size);
        }

    }
}
