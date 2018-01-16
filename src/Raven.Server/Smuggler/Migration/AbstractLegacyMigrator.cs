using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Migration
{
    public abstract class AbstractLegacyMigrator : AbstractMigrator
    {
        protected AbstractLegacyMigrator(MigratorOptions options) : base(options)
        {
        }

        protected LastEtagsInfo GetLastMigrationState()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, MigrationStateKey);
                if (document == null)
                    return null;

                return JsonDeserializationServer.OperationState(document.Data);
            }
        }

        protected LastEtagsInfo GenerateLastEtagsInfo()
        {
            var lastEtagsInfo = new LastEtagsInfo
            {
                ServerUrl = ServerUrl,
                DatabaseName = DatabaseName,
                LastDocsEtag = Result.LegacyLastDocumentEtag ?? LastEtagsInfo.EtagEmpty,
                LastAttachmentsEtag = Result.LegacyLastAttachmentEtag ?? LastEtagsInfo.EtagEmpty,
                LastDocDeleteEtag = Result.LegacyLastDocumentEtag ?? LastEtagsInfo.EtagEmpty,
                LastAttachmentsDeleteEtag = Result.LegacyLastAttachmentEtag ?? LastEtagsInfo.EtagEmpty
            };

            return lastEtagsInfo;
        }

        protected async Task SaveLastOperationState(LastEtagsInfo lastEtagsInfo)
        {
            using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var operationStateBlittable = EntityToBlittable.ConvertEntityToBlittable(lastEtagsInfo, DocumentConventions.Default, context);
                await SaveLastOperationState(operationStateBlittable);
            }
        }

        public static async Task<List<string>> GetResourcesToMigrate(string serverUrl, HttpClient httpClient, bool isRavenFs, CancellationToken cancelToken)
        {
            var url = $"{serverUrl}/{(isRavenFs ? "fs" : "databases")}";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            var response = await httpClient.SendAsync(request, cancelToken);
            if (response.StatusCode == HttpStatusCode.Unauthorized)
                throw new UnauthorizedAccessException();

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get {(isRavenFs ? "file systems" : "databases")} to migrate from server: {serverUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (var responseStream = await response.Content.ReadAsStreamAsync())
            using (var reader = new StreamReader(responseStream, Encoding.UTF8))
            {
                var jsonStr = reader.ReadToEnd();
                return JsonConvert.DeserializeObject<List<string>>(jsonStr);
            }
        }

        protected void WriteDocumentWithAttachment(IDocumentActions documentActions, DocumentsOperationContext context, Stream dataStream, string key, BlittableJsonReaderObject metadata)
        {
            using (dataStream)
            {
                var attachment = new DocumentItem.AttachmentStream
                {
                    Stream = documentActions.GetTempStream()
                };

                var attachmentDetails = StreamSource.GenerateLegacyAttachmentDetails(context, dataStream, key, metadata, ref attachment);

                var dummyDoc = new DocumentItem
                {
                    Document = new Document
                    {
                        Data = StreamSource.WriteDummyDocumentForAttachment(context, attachmentDetails),
                        Id = attachmentDetails.Id,
                        ChangeVector = string.Empty,
                        Flags = DocumentFlags.HasAttachments,
                        NonPersistentFlags = NonPersistentDocumentFlags.FromSmuggler
                    },
                    Attachments = new List<DocumentItem.AttachmentStream>
                    {
                        attachment
                    }
                };

                documentActions.WriteDocument(dummyDoc, Result.Documents);
            }
        }
    }
}
