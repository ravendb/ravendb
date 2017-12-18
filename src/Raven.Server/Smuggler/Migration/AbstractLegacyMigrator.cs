using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Migration
{
    public abstract class AbstractLegacyMigrator : AbstractMigrator
    {
        protected AbstractLegacyMigrator(
            string migrationStateKey, 
            string serverUrl, 
            string databaseName, 
            SmugglerResult result, 
            Action<IOperationProgress> onProgress, 
            DocumentDatabase database,
            HttpClient httpClient,
            OperationCancelToken cancelToken) 
            : base(migrationStateKey, serverUrl, databaseName, result, onProgress, database, cancelToken)
        {
            HttpClient = httpClient;
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

        protected BlittableJsonReaderObject GenerateOperationState(TransactionOperationContext context)
        {
            var lastEtagsInfo = new LastEtagsInfo
            {
                LastDocsEtag = Result.LegacyLastDocumentEtag ?? LastEtagsInfo.EtagEmpty,
                LastAttachmentsEtag = Result.LegacyLastAttachmentEtag ?? LastEtagsInfo.EtagEmpty,
                LastDocDeleteEtag = Result.LegacyLastDocumentEtag ?? LastEtagsInfo.EtagEmpty,
                LastAttachmentsDeleteEtag = Result.LegacyLastAttachmentEtag ?? LastEtagsInfo.EtagEmpty,
            };

            return EntityToBlittable.ConvertEntityToBlittable(lastEtagsInfo, DocumentConventions.Default, context);
        }

        public static async Task<List<string>> GetDatabasesToMigrate(string serverUrl, HttpClient httpClient, CancellationToken cancelToken)
        {
            var url = $"{serverUrl}/databases";
            var request = new HttpRequestMessage(HttpMethod.Get, url);
            var response = await httpClient.SendAsync(request, cancelToken);
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get databases to migrate from server: {serverUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            var responseStream = await response.Content.ReadAsStreamAsync();
            using (var reader = new StreamReader(responseStream, Encoding.UTF8))
            {
                var jsonStr = reader.ReadToEnd();
                return JsonConvert.DeserializeObject<List<string>>(jsonStr);
            }
        }
    }
}
