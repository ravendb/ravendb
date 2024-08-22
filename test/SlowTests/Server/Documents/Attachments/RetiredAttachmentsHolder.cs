using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments;

public abstract class RetiredAttachmentsHolder<TSettings> : RetiredAttachmentsHolderBase
    where TSettings : ICloudBackupSettings
{
    public TSettings Settings;

    protected RetiredAttachmentsHolder(ITestOutputHelper output) : base(output)
    {
    }

    public abstract IAsyncDisposable CreateCloudSettings([CallerMemberName] string caller = null);
    protected abstract Task<List<FileInfoDetails>> GetBlobsFromCloudAndAssertForCount(TSettings settings, int expected, int timeout = 120_000);
    public abstract Task DeleteObjects(TSettings s3Settings);
    public abstract Task PutRetireAttachmentsConfiguration(DocumentStore store, TSettings settings, List<string> collections = null, string database = null);

    //TODO: egor this should be generic method (and class :) )
    //public static async Task PutRetireAttachmentsConfiguration2(DocumentStore store, AzureSettings settings, List<string> collections = null, string database = null)
    //{
    //    if (collections == null)
    //        collections = new List<string> { "Orders" };
    //    if (string.IsNullOrEmpty(database))
    //        database = store.Database;
    //    await store.Maintenance.ForDatabase(database).SendAsync(new ConfigureRetireAttachmentsOperation(new RetireAttachmentsConfiguration()
    //    {
    //        AzureSettings = settings,
    //        Disabled = false,

    //        RetirePeriods = collections.ToDictionary(x => x, x => TimeSpan.FromMinutes(3)),

    //        RetireFrequencyInSec = 1000
    //    }));
    //}

    public async ValueTask DisposeAttachmentsAndDeleteObjects()
    {
        foreach (var attachment in Attachments)
        {
            if (attachment.Stream == null)
                continue;

            try
            {
                await attachment.Stream.DisposeAsync();
            }
            catch
            {
                // ignored
            }
        }

        await DeleteObjects(Settings);
    }

    public async Task PopulateDocsWithRandomAttachments(DocumentStore store, int size, List<(string Id, string Collection)> ids, int attachmentsPerDoc, int start = 0)
    {
        // put attachments
        foreach (var (id, collection) in ids)
        {
            for (int i = 0; i < attachmentsPerDoc; i++)
            {
                var rnd = new Random();
                var b = new byte[size];
                rnd.NextBytes(b);

                var profileStream = new MemoryStream(b);
                var name = $"test_{i + start}.png";
                await store.Operations.SendAsync(new PutAttachmentOperation(id, name, profileStream, "image/png"));

                profileStream.Position = 0;
                Attachments.Add(new RetiredAttachment()
                {
                    Name = name,
                    DocumentId = id,
                    Collection = collection,
                    Stream = profileStream,
                    ContentType = "image/png"
                });
            }
        }
    }

    // add attachments storage key, RetiredKey, and hash to holder
    public void GetStorageAttachmentsMetadataFromAllAttachments(DocumentDatabase database)
    {
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        using (var _documentInfoHelper = new DocumentInfoHelper(context))

        {
            foreach (var attachment in database.DocumentsStorage.AttachmentsStorage.GetAllAttachments(context))
            {
                using (var docId = _documentInfoHelper.GetDocumentId(attachment.Key))
                {
                    var t = Attachments.FirstOrDefault(x => x.DocumentId.ToLowerInvariant() == docId && x.Name == attachment.Name);
                    Assert.NotNull(t);
                    Attachments.Remove(t);
                    t.Key = attachment.Key;
                    t.Hash = attachment.Base64Hash.ToString();
                    t.RetireAt = attachment.RetiredAt;
                    //TODO: egor I can use getcollecton method here
                    t.RetiredKey =
                        $"{Settings.RemoteFolderName}/{t.Collection}/{Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(attachment.Key))}";
                    //  $"{Settings.RemoteFolderName}/{database.Name}/{t.Collection}/{Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(attachment.Key))}";
                    Attachments.Add(t);
                }
            }

            Attachments.GroupBy(x => x.RetiredKey).ToList().ForEach(x =>
            {
                Assert.Single(x);
            });
        }
    }

    public async Task AssertAllRetiredAttachments(IDocumentStore store, List<FileInfoDetails> cloudObjects, int size)
    {
        foreach (var attachment in Attachments)
        {
            Assert.Contains(cloudObjects, x => x.FullPath.Contains(attachment.RetiredKey));

            attachment.Stream.Position = 0;
            await GetAndCompareRetiredAttachment(store, attachment.DocumentId, attachment.Name, attachment.Hash, attachment.ContentType, attachment.Stream, size);
        }
    }
}
