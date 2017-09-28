using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class RavenDB_8831:RavenTestBase
    {
        [Fact]
        public async Task ReadDocWithCompressedStringFromOneContextAndWriteToAnother()
        {
            using (var documentStore = this.GetDocumentStore())
            {
                Server.ServerStore.Observer.Suspended = true;
                var originalDoc = new Doc
                {
                    Id = "doc/1",
                    StrVal = new string(Enumerable.Repeat('.', 129).ToArray()),
                    LongByteArray = Enumerable.Repeat((byte)2, 1024).ToArray()
                };

                using (var session = documentStore.OpenAsyncSession())
                {
                    await session.StoreAsync(originalDoc);
                    await session.SaveChangesAsync();
                }

                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(documentStore.Database);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var doc = database.DocumentsStorage.Get(context, "doc/1");
                    MemoryStream ms = new MemoryStream();
                    using (var newContext = JsonOperationContext.ShortTermSingleUse())
                    using (var writer = new BlittableJsonTextWriter(newContext, ms))
                    {
                        writer.WriteDocument(newContext, doc, metadataOnly: false);
                        writer.Flush();
                        var bjro = GetReaderFromMemoryStream(ms,context);
                        var desereializedDoc = (Doc)EntityToBlittable.ConvertToEntity(typeof(Doc), null, bjro, DocumentConventions.Default);

                        Assert.Equal(originalDoc.StrVal, desereializedDoc.StrVal);
                        Assert.Equal(originalDoc.LongByteArray, originalDoc.LongByteArray);
                    }
                }
            }
        }

        private unsafe BlittableJsonReaderObject GetReaderFromMemoryStream(MemoryStream ms, JsonOperationContext context)
        {
            var buffer = ms.GetBuffer();
            fixed (byte* ptr = buffer)
            {
                ms.Position = 0;
                return context.ReadForDisk(ms, string.Empty);
            }
        }

        [Fact]
        public async Task SubscriptionShouldRespectDocumentsWithCompressedData()
        {
            using (var documentStore = this.GetDocumentStore())
            {
                Server.ServerStore.Observer.Suspended = true;
                var originalDoc = new Doc
                {
                    Id = "doc/1",
                    StrVal = new string(Enumerable.Repeat('.', 129).ToArray()),
                    LongByteArray = Enumerable.Repeat((byte)2, 1024).ToArray()
                };

                using (var session = documentStore.OpenAsyncSession())
                {
                    await session.StoreAsync(originalDoc);
                    await session.SaveChangesAsync();
                }

                var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(documentStore.Database);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var doc = database.DocumentsStorage.Get(context, "doc/1");
                    MemoryStream ms = new MemoryStream();
                    using (var newContext = JsonOperationContext.ShortTermSingleUse())
                    using (var writer = new BlittableJsonTextWriter(newContext, ms))
                    {
                        writer.WriteDocument(newContext, doc, metadataOnly: false);
                        writer.Flush();
                        var bjro = GetReaderFromMemoryStream(ms, context);
                        var desereializedDoc = (Doc)EntityToBlittable.ConvertToEntity(typeof(Doc), null, bjro, DocumentConventions.Default);

                        Assert.Equal(originalDoc.StrVal, desereializedDoc.StrVal);
                        Assert.Equal(originalDoc.LongByteArray, originalDoc.LongByteArray);
                    }
                }

                var subscriptionCreationParams = new SubscriptionCreationOptions
                {
                    Query = "from Docs",
                };

                var subsId = await documentStore.Subscriptions.CreateAsync(subscriptionCreationParams).ConfigureAwait(false);
                Console.WriteLine(subsId);
                var amre = new AsyncManualResetEvent();
                using (var subscription = documentStore.Subscriptions.Open<Doc>(new SubscriptionConnectionOptions(subsId)))
                {
                    var t = subscription.Run(batch =>
                    {
                        var receivedDoc = batch.Items.First().Result;
                        Assert.Equal(originalDoc.LongByteArray, receivedDoc.LongByteArray);
                        Assert.Equal(originalDoc.StrVal, receivedDoc.StrVal);
                        amre.Set();
                    });

                    Assert.True(await amre.WaitAsync(TimeSpan.FromSeconds(60)));
                }

                
            }
        }
    }

    public class Doc
    {
        public string Id { get; set; }
        public string StrVal { get; set; }
        public byte[] LongByteArray { get; set; }
    }
}
