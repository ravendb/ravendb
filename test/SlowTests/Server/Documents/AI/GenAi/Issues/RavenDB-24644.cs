using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Json;
using Raven.Server.Documents.ETL;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    public class RavenDB_24644(ITestOutputHelper output) : RavenTestBase(output)
    {
        public record Item(PdfDescription PdfDescription = null);

        public record PdfDescription(string Description, bool SafeForWork, string[] Tags);

        private const string AttachmentName = "sample.pdf";
        private const string PdfScript = @"
ai.genContext({})
    .withPdf(loadAttachment('sample.pdf'));
";

        public const string PdfScriptWithoutNull = @"
const pdf = loadAttachment('sample.pdf');
if (!pdf) {
    return;
}

ai.genContext({}).withPdf(pdf);
";

        private const string NonEmptyAnswerHint =
            " ;Always provide a valid structured response matching the schema (if you have no answer or an empty answer - please return default values instead)";

        private const string FirstItemId = "items/1";
        private const string SecondItemId = "items/2";
        private const string ThirdItemId = "Doc/3"; //different collection to test that it won't be processed

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single,
            Data = [true])]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single,
            Data = [false])]
        public async Task SelectivePdfDescriptionTransformWhenSomeAttachmentsAreMissing(Options options, GenAiConfiguration config, bool withNullAttachments)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Prompt = "Describe the following PDFs." + NonEmptyAnswerHint;
            config.Collection = "Items";
            config.SampleObject = JsonConvert.SerializeObject(new 
            {
                PdfDescription = new
                {
                    Description = "what is written on the pdf?", 
                    SafeForWork = true, 
                    Tags = new[]
                    {
                        "matching tags for the pdf" 
                    }
                }
            });
            config.UpdateScript = @"this.PdfDescription = $output.PdfDescription;";
            config.GenAiTransformation = new GenAiTransformation { Script = withNullAttachments ? PdfScript : PdfScriptWithoutNull };

            config.EnableTracing = true;

            var taskName = (await store.Maintenance.SendAsync(new AddGenAiOperation(config))).Identifier;

            var marker = new PdfDescription("None" + Guid.NewGuid(), false, new string[] { "None" });

            var etl = Etl.WaitForEtlToComplete(store);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item(marker), FirstItemId);
                await session.StoreAsync(new Item(marker), SecondItemId);
                var doc3 = new Item(marker);
                await session.StoreAsync(doc3, ThirdItemId);
                session.Advanced.GetMetadataFor(doc3)["@collection"] = "Docs";

                await using var file1 = GetEmbeddedPdfStream("Raven.pdf");
                await using var file2 = GetEmbeddedPdfStream("Raven.pdf");
                session.Advanced.Attachments.Store(FirstItemId, AttachmentName, file1);
                session.Advanced.Attachments.Store(ThirdItemId, AttachmentName, file2);
                await session.SaveChangesAsync();
            }

            Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));
            using (var session = store.OpenAsyncSession())
            {
                var item1 = await session.LoadAsync<Item>(FirstItemId);
                var item2 = await session.LoadAsync<Item>(SecondItemId);
                var doc3 = await session.LoadAsync<Item>(ThirdItemId);

                Assert.NotNull(item1.PdfDescription);
                Assert.NotNull(item2.PdfDescription);
                Assert.NotNull(doc3.PdfDescription);
                Assert.True(doc3.PdfDescription.Description == marker.Description); // shouldn't change - because it's not in 'Items' collection

                Assert.False(item1.PdfDescription.Description == marker.Description);
                var item2Changed = ((item2.PdfDescription.Description == marker.Description) == false);
                if (withNullAttachments)
                {
                    var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    Assert.True(item2Changed || ValidateErrorNotification(db, "The request was refused by the model"));
                }
                else
                    Assert.False(item2Changed);
            }

            try
            {
                await AssertHashes(store, withNullAttachments, taskName);
            }
            catch (Exception e)
            {
                var sb = new StringBuilder();
                sb.Append("[");
                using (var session = store.OpenAsyncSession())
                {
                    var docs = await session.Advanced.LoadStartingWithAsync<dynamic>(taskName + "/");
                    foreach (var d in docs)
                    {
                        sb.AppendLine(d + ",");
                    }
                }
                sb.Append("]");
                sb.AppendLine().AppendLine("Logs:").AppendLine(await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(60)));
                sb.AppendLine().AppendLine("GenAI batch errors (which item failed + the actual exception):")
                    .AppendLine(await GetGenAiBatchErrorsAsync(store));
                throw new AggregateException("Conversation Docs: " + Environment.NewLine + sb, e);
            }
        }

        private static async Task<string> GetHash<T>(DocumentStore store, string id, string taskName)
        {
            using var session = store.OpenAsyncSession();

            var document = await session.LoadAsync<T>(id);
            var metadata = session.Advanced.GetMetadataFor(document);

            if (metadata.TryGetValue(Constants.Documents.Metadata.GenAiHashes, out object hashesSectionObj) == false)
                return null;

            if (hashesSectionObj is not MetadataAsDictionary hashesSection)
                return null;

            if (hashesSection.TryGetValue(taskName, out object hashesObj) == false)
                return null;

            if (hashesObj is not IEnumerable<object> hashesArray)
                return null;

            var firstHash = hashesArray.FirstOrDefault();

            return firstHash as string;
        }

        private async Task AssertHashes(DocumentStore store, bool withNullAttachments, string taskName)
        {
            var oldHash1 = await GetHash<Item>(store, FirstItemId, taskName);
            var oldHash2 = await GetHash<Item>(store, SecondItemId, taskName);

            Assert.NotNull(oldHash1);
            if (withNullAttachments)
                Assert.NotNull(oldHash2);
            else
                Assert.Null(oldHash2);

            var etl = await GetEtlMre(store);

            using (var session = store.OpenAsyncSession())
            {
                await using var file1 = GetEmbeddedPdfStream("Hibernating.pdf");
                await using var file2 = GetEmbeddedPdfStream("Hibernating.pdf");

                session.Advanced.Attachments.Store(FirstItemId, AttachmentName, file1);
                session.Advanced.Attachments.Store(SecondItemId, AttachmentName, file2);
                await session.SaveChangesAsync();
            }

            await EtlWait(etl);

            // Wait until hashes reflect the change
            string hash1 = string.Empty, hash2 = string.Empty;
            await WaitForAssertionAsync(async () =>
            {
                hash1 = await GetHash<Item>(store, FirstItemId, taskName);
                hash2 = await GetHash<Item>(store, SecondItemId, taskName);

                Assert.NotNull(hash1);
                Assert.False(hash2 == null, $"oldHash1={oldHash1}, oldHash2={oldHash2}, hash1={hash1}, hash2={hash2}");
                Assert.NotEqual(oldHash1, hash1);
                Assert.NotEqual(oldHash2, hash1);
            }, Debugger.IsAttached ? 1200 : 120);


            etl = await GetEtlMre(store);

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Attachments.Delete(FirstItemId, AttachmentName);
                await session.SaveChangesAsync();
            }

            await EtlWait(etl);

            await WaitForAssertionAsync(async () =>
            {
                var newHash1 = await GetHash<Item>(store, FirstItemId, taskName);
                if (withNullAttachments)
                    Assert.NotEqual(hash1, newHash1);
                else
                    Assert.Null(newHash1); // doc1 produces no context objects now - metadata hashes gets cleared
            }, Debugger.IsAttached ? 1200 : 120);
        }

        private async Task EtlWait(AsyncManualResetEvent etl)
        {
            Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));
        }

        private async Task<AsyncManualResetEvent> GetEtlMre(DocumentStore store)
        {
            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            var baselineEtag = db.EtlLoader.Processes.Single().Statistics.LastProcessedEtag;
            var etl = Etl.WaitForEtlToComplete(store, predicate: (_, statistics) => statistics.LastProcessedEtag > baselineEtag);
            return etl;
        }

        // Debug helper: surfaces, per GenAI task, exactly which request/item failed in the batch and the actual
        // exception. The GenAI ETL records this in TaskErrorsStorage:
        //  - item load errors  (GenAiTask: "Model call failed for context in document '<id>' (<ExType>). Context was: ... <full exception>")
        //  - process/batch load errors (the whole-batch AggregateException + fallback message)
        // We also dump the per-task counters so it's clear whether nothing was sent, something errored, or it stalled.
        private async Task<string> GetGenAiBatchErrorsAsync(DocumentStore store)
        {
            var sb = new StringBuilder();
            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

            foreach (var process in db.EtlLoader.Processes)
            {
                var stats = process.Statistics;
                sb.AppendLine(
                    $"Task '{process.Name}' [{process.TaskCategory}] - " +
                    $"LoadSuccesses={stats.LoadSuccesses}, LoadErrors={stats.LoadErrors}, " +
                    $"TransformationErrors={stats.TransformationErrors}, LastProcessedEtag={stats.LastProcessedEtag}");

                var itemErrors = db.TaskErrorsStorage.ReadItemErrorsOfTask(process.TaskCategory, process.Name);
                foreach (var err in itemErrors)
                    sb.AppendLine($"  ITEM  [{err.CreatedAt:O}] step={(TaskErrorStep)err.Step} doc='{err.DocumentId}'" +
                                  $"{Environment.NewLine}        {err.Error}");

                var processErrors = db.TaskErrorsStorage.ReadProcessErrorsOfTask(process.TaskCategory, process.Name);
                foreach (var err in processErrors)
                    sb.AppendLine($"  BATCH [{err.CreatedAt:O}] step={(TaskErrorStep)err.Step} affected={err.AffectedDocumentsCount}" +
                                  $"{Environment.NewLine}        {err.Error}");

                if (itemErrors.Count == 0 && processErrors.Count == 0)
                    sb.AppendLine("  (no stored load/transformation errors)");
            }

            return sb.ToString();
        }

        private static Stream GetEmbeddedPdfStream(string fileName)
        {
            var asm = typeof(RavenDB_24644).Assembly;
            var resourceName = "SlowTests.Data.RavenDB_24644." + fileName;

            var stream = asm.GetManifestResourceStream(resourceName);
            if (stream == null)
                throw new FileNotFoundException($"Embedded resource not found: {resourceName}\n" +
                                                "Check Build Action = Embedded Resource and the path/casing.");

            return stream;
        }
    }
}
