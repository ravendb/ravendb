using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Security;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Migration.ApiKey;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Migration
{
    public abstract class AbstractLegacyMigrator : AbstractMigrator
    {
        private readonly DocumentDatabase _database;

        protected AbstractLegacyMigrator(DocumentDatabase database, MigratorOptions options) : base(options)
        {
            _database = database;
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
                var operationStateBlittable = EntityToBlittable.ConvertCommandToBlittable(lastEtagsInfo, context);
                await SaveLastOperationState(operationStateBlittable);
            }
        }

        public static async Task<List<string>> GetResourcesToMigrate(
            string serverUrl,
            HttpClient httpClient,
            bool isRavenFs,
            string apiKey,
            bool enableBasicAuthenticationOverUnsecuredHttp,
            bool skipServerCertificateValidation,
            Reference<bool> isLegacyOAuthToken,
            CancellationToken cancelToken)
        {
            var response = await RunWithAuthRetryInternal(async () =>
            {
                var url = $"{serverUrl}/{(isRavenFs ? "fs" : "databases")}";
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                var responseMessage = await httpClient.SendAsync(request, cancelToken);
                return responseMessage;
            }, apiKey, serverUrl, enableBasicAuthenticationOverUnsecuredHttp, skipServerCertificateValidation, httpClient, isLegacyOAuthToken);
            
            if (response.StatusCode == HttpStatusCode.Unauthorized)
                throw new AuthorizationException();

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
                        NonPersistentFlags = NonPersistentDocumentFlags.FromSmuggler,
                        LastModified = _database.Time.GetUtcNow(),
                    },
                    Attachments = new List<DocumentItem.AttachmentStream>
                    {
                        attachment
                    }
                };

                documentActions.WriteDocument(dummyDoc, Result.Documents);
            }
        }

        protected async Task<HttpResponseMessage> RunWithAuthRetry(Func<Task<HttpResponseMessage>> requestOperation)
        {
            return await RunWithAuthRetryInternal(requestOperation, ApiKey, ServerUrl, EnableBasicAuthenticationOverUnsecuredHttp, SkipServerCertificateValidation, HttpClient);
        }

        private static async Task<HttpResponseMessage> RunWithAuthRetryInternal(
            Func<Task<HttpResponseMessage>> requestOperation,
            string apiKey,
            string serverUrl,
            bool enableBasicAuthenticationOverUnsecuredHttp,
            bool skipServerCertificateValidation,
            HttpClient httpClient,
            Reference<bool> isLegacyOAuthToken = null)
        {
            if (string.IsNullOrWhiteSpace(apiKey))
            {
                return await requestOperation().ConfigureAwait(false);
            }

            var retries = 0;
            while (true)
            {
                var response = await requestOperation().ConfigureAwait(false);
                
                if (response.StatusCode == HttpStatusCode.Unauthorized ||
                    response.StatusCode == HttpStatusCode.PreconditionFailed)
                {
                    if (++retries >= 3)
                    {
                        using (var responseStream = await response.Content.ReadAsStreamAsync())
                        using (var reader = new StreamReader(responseStream, Encoding.UTF8))
                        {
                            var str = reader.ReadToEnd();
                            throw new InvalidOperationException(str);
                        }
                    }
                        

                    var oAuthToken = await GetOAuthToken(response, apiKey, serverUrl, enableBasicAuthenticationOverUnsecuredHttp, skipServerCertificateValidation, isLegacyOAuthToken).ConfigureAwait(false);
                    SetAuthorization(httpClient, oAuthToken);
                    continue;
                }

                return response;
            }
        }

        private static async Task<string> GetOAuthToken(
            HttpResponseMessage unauthorizedResponse,
            string apiKey,
            string serverUrl,
            bool enableBasicAuthenticationOverUnsecuredHttp,
            bool skipServerCertificateValidation,
            Reference<bool> isLegacyOAuthToken)
        {
            var oauthSource = unauthorizedResponse.Headers.GetFirstValue("OAuth-Source");

            // Legacy support
            if (string.IsNullOrEmpty(oauthSource) == false &&
                oauthSource.EndsWith("/OAuth/API-Key", StringComparison.CurrentCultureIgnoreCase) == false)
            {
                if (isLegacyOAuthToken != null)
                    isLegacyOAuthToken.Value = true;

                return await Authenticator.GetLegacyOAuthToken(
                    oauthSource, apiKey, enableBasicAuthenticationOverUnsecuredHttp);
            }

            if (string.IsNullOrEmpty(oauthSource))
                oauthSource = serverUrl + "/OAuth/API-Key";

            return await Authenticator.GetOAuthToken(serverUrl, oauthSource, apiKey, skipServerCertificateValidation);
        }

        private static void SetAuthorization(HttpClient httpClient, string oAuthToken)
        {
            try
            {
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", oAuthToken);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Could not set the Authorization to the value 'Bearer {oAuthToken}'", ex);
            }
        }
    }
}
