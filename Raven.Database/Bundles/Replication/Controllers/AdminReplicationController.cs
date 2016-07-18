using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Json;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Streaming;
using Raven.Abstractions.Util;
using Raven.Database.Actions;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Database.Server.Controllers;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Controllers
{
    public class AdminReplicationController : AdminBundlesApiController
    {
        public override string BundleName
        {
            get { return "replication"; }
        }

        [HttpPost]
        [RavenRoute("admin/replication/purge-tombstones")]
        [RavenRoute("databases/{databaseName}/admin/replication/purge-tombstones")]
        public HttpResponseMessage PurgeTombstones()
        {
            var docEtagStr = GetQueryStringValue("docEtag");
            var attachmentEtagStr = GetQueryStringValue("attachmentEtag");

            Etag docEtag, attachmentEtag;

            var docEtagParsed = Etag.TryParse(docEtagStr, out docEtag);
            var attachmentEtagParsed = Etag.TryParse(attachmentEtagStr, out attachmentEtag);

            if (docEtagParsed == false && attachmentEtagParsed == false)
            {
                return GetMessageWithObject(
                    new
                    {
                        Error = "The query string variable 'docEtag' or 'attachmentEtag' must be set to a valid etag"
                    }, HttpStatusCode.BadRequest);
            }

            Database.TransactionalStorage.Batch(accessor =>
            {
                if (docEtag != null)
                {
                    accessor.Lists.RemoveAllBefore(Constants.RavenReplicationDocsTombstones, docEtag);
                }
                if (attachmentEtag != null)
                {
                    accessor.Lists.RemoveAllBefore(Constants.RavenReplicationAttachmentsTombstones, attachmentEtag);
                }
            });

            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("admin/replicationInfo")]
        [RavenRoute("databases/{databaseName}/admin/replicationInfo")]
        public async Task<HttpResponseMessage> ReplicationInfo()
        {
            var replicationDocument = await ReadJsonObjectAsync<ReplicationDocument>().ConfigureAwait(false);

            if (replicationDocument == null || replicationDocument.Destinations == null || replicationDocument.Destinations.Count == 0)
            {
                return GetMessageWithObject(new
                {
                    Error = "Invalid `ReplicationDocument` document supplied."
                }, HttpStatusCode.BadRequest);
            }

            var statuses = CheckDestinations(replicationDocument);

            return GetMessageWithObject(statuses);
        }

        [HttpPost]
        [RavenRoute("admin/replication/topology/view")]
        [RavenRoute("databases/{databaseName}/admin/replication/topology/view")]
        public Task<HttpResponseMessage> ReplicationTopology()
        {
            var replicationSchemaDiscoverer = new ReplicationTopologyDiscoverer(Database, new RavenJArray(), 10, Log);
            var node = replicationSchemaDiscoverer.Discover();
            var topology = node.Flatten();

            return GetMessageWithObjectAsTask(topology);
        }

        [HttpPost]
        [RavenRoute("admin/replication/docs-left-to-replicate")]
        [RavenRoute("databases/{databaseName}/admin/replication/docs-left-to-replicate")]
        public async Task<HttpResponseMessage> DocumentsLeftToReplicate()
        {
            var serverInfo = await ReadJsonObjectAsync<ServerInfo>().ConfigureAwait(false);
            var documentsToReplicateCalculator = new DocumentsLeftToReplicate(Database);
            var documentsToReplicate = documentsToReplicateCalculator.Calculate(serverInfo);

            return await GetMessageWithObjectAsTask(documentsToReplicate).ConfigureAwait(false);
        }

        [HttpPost]
        [RavenRoute("admin/replication/replicated-docs-by-entity-names")]
        [RavenRoute("databases/{databaseName}/admin/replication/replicated-docs-by-entity-names")]
        public async Task<HttpResponseMessage> ReplicatedDocumentsByEntityNames()
        {
            var query = await ReadJsonObjectAsync<string>().ConfigureAwait(false);
            var documentsToReplicateCalculator = new DocumentsLeftToReplicate(Database);
            var documentsToReplicateCount = documentsToReplicateCalculator.GetDocumentCountForEntityNames(query);

            return await GetMessageWithObjectAsTask(documentsToReplicateCount).ConfigureAwait(false);
        }

        [HttpPost]
        [RavenRoute("admin/replication/export-docs-left-to-replicate")]
        [RavenRoute("databases/{databaseName}/admin/replication/export-docs-left-to-replicate")]
        public Task<HttpResponseMessage> ExportDocumentsLeftToReplicate([FromBody] StudioTasksController.ExportData optionsJson)
        {
            var result = GetEmptyMessage();
            var taskId = optionsJson.ProgressTaskId;
            var status = new OperationStatus();

            var tcs = new TaskCompletionSource<object>();
            try
            {
                var sp = Stopwatch.StartNew();

                Database.Tasks.AddTask(tcs.Task, status, new TaskActions.PendingTaskDescription
                {
                    StartTime = SystemTime.UtcNow,
                    TaskType = TaskActions.PendingTaskType.ExportDocumentsLeftToReplicate,
                    Description = "Monitoring progress of export of docs left to replicate"
                }, taskId, null, skipStatusCheck: true);

                var requestString = optionsJson.DownloadOptions;
                ServerInfo serverInfo;

                using (var jsonReader = new RavenJsonTextReader(new StringReader(requestString)))
                {
                    var serializer = JsonExtensions.CreateDefaultJsonSerializer();
                    serverInfo = (ServerInfo)serializer.Deserialize(jsonReader, typeof(ServerInfo));
                }

                var documentsToReplicateCalculator = new DocumentsLeftToReplicate(Database);

                if (serverInfo.SourceId != documentsToReplicateCalculator.DatabaseId)
                {
                    throw new InvalidOperationException("Cannot export documents to replicate from a server other than this one!");
                }

                // create PushStreamContent object that will be called when the output stream will be ready.
                result.Content = new PushStreamContent((outputStream, content, arg3) =>
                {
                    using (var writer = new ExcelOutputWriter(outputStream))
                    {
                        try
                        {
                            writer.WriteHeader();
                            writer.Write("document-ids-left-to-replicate");

                            var count = 0;

                            Action<string> action = (documentId) =>
                            {
                                writer.Write(documentId);

                                if (++count%1000 == 0)
                                {
                                    status.MarkProgress($"Exported {count:#,#} documents");
                                    outputStream.Flush();
                                }
                            };

                            documentsToReplicateCalculator.ExtractDocumentIds(serverInfo, action);

                            var message = $"Completed export of {count:#,#} document ids";
                            status.MarkCompleted(message, sp.Elapsed);
                        }
                        catch (Exception e)
                        {
                            status.ExceptionDetails = e.ToString();
                            status.MarkFaulted(e.ToString());
                            writer.WriteError(e);
                            throw;
                        }
                        finally
                        {
                            tcs.SetResult("Completed");
                            outputStream.Close();
                        }
                    }
                });

                var fileName = $"Documents to replicate from '{serverInfo.SourceUrl}' to '{serverInfo.DestinationUrl}', " +
                                $"{DateTime.Now.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture)}";

                foreach (char c in Path.GetInvalidFileNameChars())
                {
                    fileName = fileName.Replace(c, '_');
                }

                result.Content.Headers.ContentDisposition = new ContentDispositionHeaderValue("attachment")
                {
                    FileName = fileName + ".csv"
                };
            }
            catch (Exception e)
            {
                status.ExceptionDetails = e.ToString();
                status.MarkFaulted(e.ToString());
                tcs.SetResult("Completed");
                throw;
            }
            
            return new CompletedTask<HttpResponseMessage>(result);
        }

        private class OperationStatus : OperationStateBase
        {
            public string ExceptionDetails { get; set; }
        }

        private ReplicationInfoStatus[] CheckDestinations(ReplicationDocument replicationDocument)
        {
            var results = new ReplicationInfoStatus[replicationDocument.Destinations.Count];

            Parallel.ForEach(replicationDocument.Destinations, (replicationDestination, state, i) =>
            {
                var url = replicationDestination.Url;

                if (!url.ToLower().Contains("/databases/"))
                {
                    url += "/databases/" + replicationDestination.Database;
                }

                var result = new ReplicationInfoStatus
                {
                    Url = url,
                    Status = "Valid",
                    Code = (int)HttpStatusCode.OK
                };

                results[i] = result;

                var ravenConnectionStringOptions = new RavenConnectionStringOptions
                {
                    ApiKey = replicationDestination.ApiKey,
                    DefaultDatabase = replicationDestination.Database,
                };
                if (string.IsNullOrEmpty(replicationDestination.Username) == false)
                {
                    ravenConnectionStringOptions.Credentials = new NetworkCredential(replicationDestination.Username,
                                                                                     replicationDestination.Password,
                                                                                     replicationDestination.Domain ?? string.Empty);
                }

                var requestFactory = new HttpRavenRequestFactory();
                var request = requestFactory.Create(url + "/replication/info", HttpMethods.Post, ravenConnectionStringOptions);
                try
                {
                    request.ExecuteRequest();
                }
                catch (WebException e)
                {
                    FillStatus(result, e);
                }
            });

            return results;
        }

        private void FillStatus(ReplicationInfoStatus replicationInfoStatus, WebException e)
        {
            if (e.GetBaseException() is WebException)
                e = (WebException)e.GetBaseException();

            var response = e.Response as HttpWebResponse;
            if (response == null)
            {
                replicationInfoStatus.Status = e.Message;
                replicationInfoStatus.Code = -1 * (int)e.Status;
                return;
            }

            switch (response.StatusCode)
            {
                case HttpStatusCode.BadRequest:
                    string error = GetErrorStringFromException(e, response);
                    replicationInfoStatus.Status = error.Contains("replication bundle not activated")
                                                           ? "Replication bundle not activated."
                                                           : error;
                    replicationInfoStatus.Code = (int)response.StatusCode;
                    break;
                case HttpStatusCode.PreconditionFailed:
                    replicationInfoStatus.Status = "Could not authenticate using OAuth's API Key";
                    replicationInfoStatus.Code = (int)response.StatusCode;
                    break;
                case HttpStatusCode.Forbidden:
                case HttpStatusCode.Unauthorized:
                    replicationInfoStatus.Status = "Could not authenticate using Windows Auth";
                    replicationInfoStatus.Code = (int)response.StatusCode;
                    break;
                default:
                    replicationInfoStatus.Status = response.StatusDescription;
                    replicationInfoStatus.Code = (int)response.StatusCode;
                    break;
            }
        }


        private static string GetErrorStringFromException(WebException webException, HttpWebResponse response)
        {
            var s = webException.Data["original-value"] as string;
            if (s != null)
                return s;
            using (var streamReader = new StreamReader(response.GetResponseStreamWithHttpDecompression()))
            {
                return streamReader.ReadToEnd();
            }
        }
    }
}
