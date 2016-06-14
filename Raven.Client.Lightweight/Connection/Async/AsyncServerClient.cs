//-----------------------------------------------------------------------
// <copyright file="AsyncServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Connection.Implementation;
using Raven.Client.Indexes;
using Raven.Database.Data;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Json;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Listeners;
using Raven.Client.Util.Auth;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Client.Connection.Async
{
    public class AsyncServerClient : IAsyncDatabaseCommands, IAsyncInfoDatabaseCommands
    {
        private readonly ProfilingInformation profilingInformation;
        private readonly IDocumentConflictListener[] conflictListeners;
        private readonly string url;
        private readonly string rootUrl;
        private readonly OperationCredentials credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication;
        internal readonly DocumentConvention convention;
        private NameValueCollection operationsHeaders = new NameValueCollection();
        internal readonly HttpJsonRequestFactory jsonRequestFactory;
        private readonly Guid? sessionId;
        private readonly Func<string, IDocumentStoreReplicationInformer> replicationInformerGetter;
        private readonly string databaseName;
        private readonly IDocumentStoreReplicationInformer replicationInformer;
        private int requestCount;
        private int readStripingBase;

        public string Url
        {
            get { return url; }
        }

        public IDocumentStoreReplicationInformer ReplicationInformer
        {
            get { return replicationInformer; }
        }

        public OperationCredentials PrimaryCredentials
        {
            get
            {
                return credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication;
            }
        }

        public AsyncServerClient(string url, DocumentConvention convention, OperationCredentials credentials,
                                 HttpJsonRequestFactory jsonRequestFactory, Guid? sessionId,
                                 Func<string, IDocumentStoreReplicationInformer> replicationInformerGetter, string databaseName,
                                 IDocumentConflictListener[] conflictListeners, bool incrementReadStripe)
        {
            profilingInformation = ProfilingInformation.CreateProfilingInformation(sessionId);
            this.url = url;
            if (this.url.EndsWith("/"))
                this.url = this.url.Substring(0, this.url.Length - 1);
            rootUrl = this.url;
            var databasesIndex = rootUrl.IndexOf("/databases/", StringComparison.OrdinalIgnoreCase);
            if (databasesIndex > 0)
            {
                rootUrl = rootUrl.Substring(0, databasesIndex);
            }
            this.jsonRequestFactory = jsonRequestFactory;
            this.sessionId = sessionId;
            this.convention = convention;
            this.credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication = credentials;
            this.databaseName = databaseName;
            this.conflictListeners = conflictListeners;
            this.replicationInformerGetter = replicationInformerGetter;
            this.replicationInformer = replicationInformerGetter(databaseName);
            this.readStripingBase = replicationInformer.GetReadStripingBase(incrementReadStripe);

            this.replicationInformer.UpdateReplicationInformationIfNeeded(this);
        }

        public void Dispose()
        {
        }

        public Task<string[]> GetIndexNamesAsync(int start, int pageSize, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.IndexNames(start, pageSize), "GET", operationMetadata.Credentials, convention)).AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
                {
                    var json = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return json.Select(x => x.Value<string>()).ToArray();
                }
            }, token);
        }

        public Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                var operationUrl = operationMetadata.Url + "/indexes/?start=" + start + "&pageSize=" + pageSize;
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl, "GET", operationMetadata.Credentials, convention)))
                {
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    var json = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    //NOTE: To review, I'm not confidence this is the correct way to deserialize the index definition
                    return json.Select(x =>
                    {
                        var value = ((RavenJObject)x)["definition"].ToString();
                        return JsonConvert.DeserializeObject<IndexDefinition>(value, new JsonToJsonConverter());
                    }).ToArray();
                }
            }, token);
        }

        public Task<TransformerDefinition[]> GetTransformersAsync(int start, int pageSize, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                var operationUrl = operationMetadata.Url + "/transformers?start=" + start + "&pageSize=" + pageSize;
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl, "GET", operationMetadata.Credentials, convention)))
                {
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    var json = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);

                    //NOTE: To review, I'm not confidence this is the correct way to deserialize the transformer definition
                    return json.Select(x => JsonConvert.DeserializeObject<TransformerDefinition>(((RavenJObject)x)["definition"].ToString(), new JsonToJsonConverter())).ToArray();
                }
            }, token);
        }

        public Task SetTransformerLockAsync(string name, TransformerLockMode lockMode, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("POST", async operationMetadata =>
            {
                var operationUrl = operationMetadata.Url + "/transformers/" + name + "?op=" + "lockModeChange" + "&mode=" + lockMode;
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl, "POST", operationMetadata.Credentials, convention)))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        public Task ResetIndexAsync(string name, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("RESET", async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/indexes/" + name, "RESET", operationMetadata.Credentials, convention)))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        public Task SetIndexLockAsync(string name, IndexLockMode unLockMode, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("POST", async operationMetadata =>
            {
                var operationUrl = operationMetadata.Url + "/indexes/" + name + "?op=" + "lockModeChange" + "&mode=" + unLockMode;
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl, "POST", operationMetadata.Credentials, convention)))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }
        public Task SetIndexPriorityAsync(string name, IndexingPriority priority, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("POST", async operationMetadata =>
            {
                var operationUrl = operationMetadata.Url + "/indexes/set-priority/" + name + "?priority=" + priority;
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl, "POST", operationMetadata.Credentials, convention)))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }
        public Task<string> PutIndexAsync<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, CancellationToken token = default(CancellationToken))
        {
            return PutIndexAsync(name, indexDef, false, token);
        }

        public Task<string> PutIndexAsync<TDocument, TReduceResult>(string name,
                     IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite = false, CancellationToken token = default(CancellationToken))
        {
            return PutIndexAsync(name, indexDef.ToIndexDefinition(convention), overwrite, token);
        }

        public Task<bool> IndexHasChangedAsync(string name, IndexDefinition indexDef, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("POST", operationMetadata => DirectIndexHasChangedAsync(name, indexDef, operationMetadata, token), token);
        }

        private async Task<bool> DirectIndexHasChangedAsync(string name, IndexDefinition indexDef, OperationMetadata operationMetadata, CancellationToken token)
        {
            var requestUri = operationMetadata.Url.Indexes(name) + "?op=hasChanged";
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, "POST", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
            {
                request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                var serializeObject = JsonConvert.SerializeObject(indexDef, Default.Converters);

                await request.WriteAsync(serializeObject).WithCancellation(token).ConfigureAwait(false);
                var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return result.Value<bool>("Changed");
            }
        }

        public Task<string> PutIndexAsync(string name, IndexDefinition indexDef, CancellationToken token = default(CancellationToken))
        {
            return PutIndexAsync(name, indexDef, false, token);
        }

        public Task<string> PutIndexAsync(string name, IndexDefinition indexDef, bool overwrite, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("PUT", operationMetadata => DirectPutIndexAsync(name, indexDef, overwrite, operationMetadata, token), token);
        }

        public Task<Tuple<string, Operation>> PutIndexAsyncWithOperation(string name, IndexDefinition indexDef, bool overwrite, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("PUT", operationMetadata => DirectPutIndexAsyncWithOperation(name, indexDef, overwrite, operationMetadata, token), token);
        }


        public Task<string[]> PutIndexesAsync(IndexToAdd[] indexesToAdd, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("PUT", operationMetadata => DirectPutIndexesAsync(indexesToAdd, operationMetadata, token), token);
        }

        public Task<string[]> PutSideBySideIndexesAsync(IndexToAdd[] indexesToAdd, Etag minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("PUT", operationMetadata => DirectPutSideBySideIndexesAsync(indexesToAdd, operationMetadata, minimumEtagBeforeReplace, replaceTimeUtc, token), token);
        }

        public Task<string> PutTransformerAsync(string name, TransformerDefinition transformerDefinition, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("PUT", operationMetadata => DirectPutTransformerAsync(name, transformerDefinition, operationMetadata, token), token);
        }

        public async Task<string> DirectPutIndexAsync(string name, IndexDefinition indexDef, bool overwrite, OperationMetadata operationMetadata, CancellationToken token = default(CancellationToken))
        {
            return (await DirectPutIndexAsyncWithOperation(name, indexDef, overwrite, operationMetadata, token).ConfigureAwait(false)).Item1;
        }

        public async Task<Tuple<string, Operation>> DirectPutIndexAsyncWithOperation(string name, IndexDefinition indexDef, bool overwrite, OperationMetadata operationMetadata, CancellationToken token = default(CancellationToken))
        {
            var requestUri = operationMetadata.Url + "/indexes/" + Uri.EscapeUriString(name) + "?definition=yes&includePrecomputeOperation=yes";
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, "GET", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
            {
                request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                try
                {
                    await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                    if (overwrite == false)
                        throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode != HttpStatusCode.NotFound)
                        throw;
                }
            }

            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, "PUT", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
            {
                var serializeObject = JsonConvert.SerializeObject(indexDef, Default.Converters);

                ErrorResponseException responseException;
                try
                {
                    await request.WriteAsync(serializeObject).ConfigureAwait(false);
                    var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    var resultObject = result as RavenJObject;
                    if (resultObject == null || !resultObject.ContainsKey("OperationId"))
                        return Tuple.Create(result.Value<string>("Index"), (Operation)null);

                    var operationId = result.Value<long>("OperationId");
                    return Tuple.Create(result.Value<string>("Index"), operationId != -1 ? new Operation(this, operationId) : null);
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode != HttpStatusCode.BadRequest) throw;
                    responseException = e;
                }
                var error = await responseException.TryReadErrorResponseObject(new { Error = "", Message = "", IndexDefinitionProperty = "", ProblematicText = "" }).ConfigureAwait(false);
                if (error == null) throw responseException;

                throw new IndexCompilationException(error.Message) { IndexDefinitionProperty = error.IndexDefinitionProperty, ProblematicText = error.ProblematicText };
            }
        }

        public async Task<string[]> DirectPutIndexesAsync(IndexToAdd[] indexesToAdd, OperationMetadata operationMetadata, CancellationToken token = default(CancellationToken))
        {
            var requestUri = operationMetadata.Url + "/indexes";
            return await PutIndexes(operationMetadata, token, requestUri, indexesToAdd).ConfigureAwait(false);
        }

        public async Task<string[]> DirectPutSideBySideIndexesAsync(IndexToAdd[] indexesToAdd, OperationMetadata operationMetadata, Etag minimumEtagBeforeReplace, DateTime? replaceTimeUtc, CancellationToken token = default(CancellationToken))
        {
            var sideBySideIndexes = new SideBySideIndexes
            {
                IndexesToAdd = indexesToAdd,
                MinimumEtagBeforeReplace = minimumEtagBeforeReplace,
                ReplaceTimeUtc = replaceTimeUtc
            };

            var requestUri = operationMetadata.Url + "/side-by-side-indexes";
            return await PutIndexes(operationMetadata, token, requestUri, sideBySideIndexes).ConfigureAwait(false);
        }

        private async Task<string[]> PutIndexes(OperationMetadata operationMetadata, CancellationToken token, string requestUri, object obj)
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, "PUT", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
            {
                var serializeObject = JsonConvert.SerializeObject(obj, Default.Converters);

                ErrorResponseException responseException;
                try
                {
                    await request.WriteAsync(serializeObject).WithCancellation(token).ConfigureAwait(false);
                    var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    return result
                        .Value<RavenJArray>("Indexes")
                        .Select(x => x.Value<string>())
                        .ToArray();
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode != HttpStatusCode.BadRequest)
                        throw;
                    responseException = e;
                }
                var error = await responseException.TryReadErrorResponseObject(new { Error = "", Message = "", IndexDefinitionProperty = "", ProblematicText = "" }).ConfigureAwait(false);
                if (error == null)
                    throw responseException;

                throw new IndexCompilationException(error.Message) { IndexDefinitionProperty = error.IndexDefinitionProperty, ProblematicText = error.ProblematicText };
            }
        }

        public async Task<string> DirectPutTransformerAsync(string name, TransformerDefinition transformerDefinition,
                                                            OperationMetadata operationMetadata, CancellationToken token = default(CancellationToken))
        {
            var requestUri = operationMetadata.Url + "/transformers/" + name;

            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, "PUT", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
            {
                var serializeObject = JsonConvert.SerializeObject(transformerDefinition, Default.Converters);

                ErrorResponseException responseException;
                try
                {
                    await request.WriteAsync(serializeObject).ConfigureAwait(false);
                    var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return result.Value<string>("Transformer");
                }
                catch (BadRequestException e)
                {
                    throw new TransformCompilationException(e.Message);
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode != HttpStatusCode.BadRequest)
                        throw;
                    responseException = e;
                }
                var error = await responseException.TryReadErrorResponseObject(new { Error = "", Message = "" }).ConfigureAwait(false);
                if (error == null)
                    throw responseException;

                throw new TransformCompilationException(error.Message);
            }
        }

        public Task DeleteIndexAsync(string name, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("DELETE", async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Indexes(name), "DELETE", operationMetadata.Credentials, convention).AddOperationHeaders(operationsHeaders)))
                {
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);
                    await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        public Task<Operation> DeleteByIndexAsync(string indexName, IndexQuery queryToDelete, BulkOperationOptions options = null, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("DELETE", async operationMetadata =>
            {
                var notNullOptions = options ?? new BulkOperationOptions();
                string path = queryToDelete.GetIndexQueryUrl(operationMetadata.Url, indexName, "bulk_docs") + "&allowStale=" + notNullOptions.AllowStale
                     + "&details=" + notNullOptions.RetrieveDetails;
                if (notNullOptions.MaxOpsPerSec != null)
                    path += "&maxOpsPerSec=" + notNullOptions.MaxOpsPerSec;
                if (notNullOptions.StaleTimeout != null)
                    path += "&staleTimeout=" + notNullOptions.StaleTimeout;

                token.ThrowCancellationIfNotDefault(); //maybe the operation is canceled and we can spare the request..
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path, "DELETE", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);
                    RavenJToken jsonResponse;
                    try
                    {
                        jsonResponse = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode == HttpStatusCode.NotFound) throw new InvalidOperationException("There is no index named: " + indexName, e);
                        throw;
                    }

                    // Be compatible with the response from v2.0 server
                    var opId = ((RavenJObject)jsonResponse)["OperationId"];

                    if (opId == null || opId.Type != JTokenType.Integer) return null;

                    return new Operation(this, opId.Value<long>());
                }
            }, token);
        }

        public Task DeleteTransformerAsync(string name, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("DELETE", async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Transformer(name), "DELETE", operationMetadata.Credentials, convention).AddOperationHeaders(operationsHeaders)))
                {
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);
                    await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        public Task<RavenJObject> PatchAsync(string key, PatchRequest[] patches, CancellationToken token = default(CancellationToken))
        {
            return PatchAsync(key, patches, null, token);
        }

        public async Task<RavenJObject> PatchAsync(string key, PatchRequest[] patches, bool ignoreMissing, CancellationToken token = default(CancellationToken))
        {
            var batchResults = await BatchAsync(new ICommandData[]
                    {
                        new PatchCommandData
                            {
                                Key = key,
                                Patches = patches,
                            }
                    }, token).ConfigureAwait(false);
            if (!ignoreMissing && batchResults[0].PatchResult != null &&
                batchResults[0].PatchResult == PatchResult.DocumentDoesNotExists)
                throw new DocumentDoesNotExistsException("Document with key " + key + " does not exist.");
            return batchResults[0].AdditionalData;
        }

        public Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patch, CancellationToken token = default(CancellationToken))
        {
            return PatchAsync(key, patch, null, token);
        }

        public async Task<RavenJObject> PatchAsync(string key, PatchRequest[] patches, Etag etag, CancellationToken token = default(CancellationToken))
        {
            var batchResults = await BatchAsync(new ICommandData[]
                    {
                        new PatchCommandData
                            {
                                Key = key,
                                Patches = patches,
                                Etag = etag,
                            }
                    }, token).ConfigureAwait(false);
            return batchResults[0].AdditionalData;
        }

        public async Task<RavenJObject> PatchAsync(string key, PatchRequest[] patchesToExisting,
                                                   PatchRequest[] patchesToDefault, RavenJObject defaultMetadata, CancellationToken token = default(CancellationToken))
        {
            var batchResults = await BatchAsync(new ICommandData[]
                    {
                        new PatchCommandData
                            {
                                Key = key,
                                Patches = patchesToExisting,
                                PatchesIfMissing = patchesToDefault,
                                Metadata = defaultMetadata
                            }
                    }, token).ConfigureAwait(false);
            return batchResults[0].AdditionalData;
        }

        public async Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patch, bool ignoreMissing, CancellationToken token = default(CancellationToken))
        {
            var batchResults = await BatchAsync(new ICommandData[]
            {
                new ScriptedPatchCommandData
                {
                    Key = key,
                    Patch = patch,
                }
            }, token).ConfigureAwait(false);
            if (!ignoreMissing && batchResults[0].PatchResult != null &&
                batchResults[0].PatchResult == PatchResult.DocumentDoesNotExists)
                throw new DocumentDoesNotExistsException("Document with key " + key + " does not exist.");
            return batchResults[0].AdditionalData;
        }

        public async Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patch, Etag etag, CancellationToken token = default(CancellationToken))
        {
            var batchResults = await BatchAsync(new ICommandData[]
            {
                new ScriptedPatchCommandData
                {
                    Key = key,
                    Patch = patch,
                    Etag = etag
                }
            }, token).ConfigureAwait(false);
            return batchResults[0].AdditionalData;
        }

        public async Task<RavenJObject> PatchAsync(string key, ScriptedPatchRequest patchExisting,
                                                   ScriptedPatchRequest patchDefault, RavenJObject defaultMetadata, CancellationToken token = default(CancellationToken))
        {
            var batchResults = await BatchAsync(new ICommandData[]
            {
                new ScriptedPatchCommandData
                {
                    Key = key,
                    Patch = patchExisting,
                    PatchIfMissing = patchDefault,
                    Metadata = defaultMetadata
                }
            }, token).ConfigureAwait(false);
            return batchResults[0].AdditionalData;
        }

        public Task<PutResult> PutAsync(string key, Etag etag, RavenJObject document, RavenJObject metadata, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("PUT", operationMetadata => DirectPutAsync(operationMetadata, key, etag, document, metadata, token), token);
        }

        private async Task<PutResult> DirectPutAsync(OperationMetadata operationMetadata, string key, Etag etag, RavenJObject document, RavenJObject metadata, CancellationToken token = default(CancellationToken))
        {
            if (metadata == null)
                metadata = new RavenJObject();
            var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
            if (etag != null)
                metadata[Constants.MetadataEtagField] = new RavenJValue((string)etag);
            else
                metadata.Remove(Constants.MetadataEtagField);

            if (key != null)
                key = Uri.EscapeDataString(key);

            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/docs/" + key, method, metadata, operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
            {
                request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                ErrorResponseException responseException;
                try
                {
                    await request.WriteAsync(document).WithCancellation(token).ConfigureAwait(false);
                    var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    if (result == null)
                    {
                        throw new InvalidOperationException("Got null response from the server after doing a put on " + key + ", something is very wrong. Probably a garbled response.");
                    }
                    return convention.CreateSerializer().Deserialize<PutResult>(new RavenJTokenReader(result));
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode != HttpStatusCode.Conflict) throw;
                    responseException = e;
                }
                throw FetchConcurrencyException(responseException);
            }
        }

        public IAsyncDatabaseCommands ForDatabase(string database)
        {
            return ForDatabaseInternal(database);
        }

        public IAsyncDatabaseCommands ForSystemDatabase()
        {
            return ForSystemDatabaseInternal();
        }

        internal AsyncServerClient ForDatabaseInternal(string database)
        {
            if (database == Constants.SystemDatabase)
                return ForSystemDatabaseInternal();

            var databaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
            databaseUrl = databaseUrl + "/databases/" + database + "/";
            if (databaseUrl == url)
                return this;

            return new AsyncServerClient(databaseUrl, convention, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, jsonRequestFactory, sessionId, replicationInformerGetter, database, conflictListeners, false) { operationsHeaders = operationsHeaders };
        }

        internal AsyncServerClient ForSystemDatabaseInternal()
        {
            var databaseUrl = MultiDatabase.GetRootDatabaseUrl(url);
            if (databaseUrl == url)
                return this;

            return new AsyncServerClient(databaseUrl, convention, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, jsonRequestFactory, sessionId, replicationInformerGetter, databaseName, conflictListeners, false) { operationsHeaders = operationsHeaders };
        }

        public NameValueCollection OperationsHeaders
        {
            get { return operationsHeaders; }
            set { operationsHeaders = value; }
        }

        public IAsyncGlobalAdminDatabaseCommands GlobalAdmin { get { return new AsyncAdminServerClient(this); } }

        public IAsyncAdminDatabaseCommands Admin { get { return new AsyncAdminServerClient(this); } }

        public Task<JsonDocument> GetAsync(string key, CancellationToken token = default(CancellationToken))
        {
            EnsureIsNotNullOrEmpty(key, "key");

            return ExecuteWithReplication("GET", operationMetadata => DirectGetAsync(operationMetadata, key, token), token);
        }

        public Task<TransformerDefinition> GetTransformerAsync(string name, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                try
                {
                    using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Transformer(name), "GET", operationMetadata.Credentials, convention)).AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
                    {
                        var transformerDefinitionJson = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                        var value = transformerDefinitionJson.Value<RavenJObject>("Transformer");
                        return convention.CreateSerializer().Deserialize<TransformerDefinition>(new RavenJTokenReader(value));
                    }
                }
                catch (ErrorResponseException we)
                {
                    if (we.StatusCode == HttpStatusCode.NotFound)
                        return null;

                    throw;
                }
            }, token);
        }

        public Task<IndexDefinition> GetIndexAsync(string name, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                try
                {
                    using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.IndexDefinition(name), "GET", operationMetadata.Credentials, convention)).AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
                    {
                        var indexDefinitionJson = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                        var value = indexDefinitionJson.Value<RavenJObject>("Index");
                        return convention.CreateSerializer().Deserialize<IndexDefinition>(new RavenJTokenReader(value));
                    }
                }
                catch (ErrorResponseException we)
                {
                    if (we.StatusCode == HttpStatusCode.NotFound)
                        return null;

                    throw;
                }

            }, token);
        }

        public async Task<JsonDocument> DirectGetAsync(OperationMetadata operationMetadata, string key, CancellationToken token)
        {
            if (key.Length > 127)
            {
                // avoid hitting UrlSegmentMaxLength limits in Http.sys
                var multiLoadResult = await DirectGetAsync(operationMetadata, new[] { key }, new string[0], null, new Dictionary<string, RavenJToken>(), false, token).WithCancellation(token).ConfigureAwait(false);
                var result = multiLoadResult.Results.FirstOrDefault();
                if (result == null)
                    return null;
                return SerializationHelper.RavenJObjectToJsonDocument(result);
            }

            var metadata = new RavenJObject();
            AddTransactionInformation(metadata);
            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, (operationMetadata.Url + "/docs?id=" + Uri.EscapeDataString(key)), "GET", metadata, operationMetadata.Credentials, convention);
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams.AddOperationHeaders(OperationsHeaders)).AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
            {
                Task<JsonDocument> resolveConflictTask;
                try
                {
                    var requestJson = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    var docKey = request.ResponseHeaders.Get(Constants.DocumentIdFieldName) ?? key;
                    docKey = Uri.UnescapeDataString(docKey);
                    request.ResponseHeaders.Remove(Constants.DocumentIdFieldName);
                    var deserializeJsonDocument = SerializationHelper.DeserializeJsonDocument(docKey, requestJson, request.ResponseHeaders, request.ResponseStatusCode);
                    return deserializeJsonDocument;
                }
                catch (ErrorResponseException e)
                {
                    switch (e.StatusCode)
                    {
                        case HttpStatusCode.NotFound:
                            return null;
                        case HttpStatusCode.Conflict:
                            resolveConflictTask = ResolveConflict(e.ResponseString, e.Etag, operationMetadata, key, token);
                            break;
                        default:
                            throw;
                    }
                }
                return await resolveConflictTask.WithCancellation(token).ConfigureAwait(false);
            }
        }

        private async Task<JsonDocument> ResolveConflict(string httpResponse, Etag etag, OperationMetadata operationMetadata, string key, CancellationToken token)
        {
            var conflicts = new StringReader(httpResponse);
            var conflictsDoc = RavenJObject.Load(new RavenJsonTextReader(conflicts));
            var result =
                await TryResolveConflictOrCreateConcurrencyException(operationMetadata, key, conflictsDoc, etag, token).ConfigureAwait(false);
            if (result != null)
                throw result;
            return await DirectGetAsync(operationMetadata, key, token).ConfigureAwait(false);
        }

        public Task<MultiLoadResult> GetAsync(string[] keys, string[] includes, string transformer = null,
                                              Dictionary<string, RavenJToken> transformerParameters = null, bool metadataOnly = false, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", operationMetadata => DirectGetAsync(operationMetadata, keys, includes, transformer, transformerParameters, metadataOnly, token), token);
        }

        private async Task<MultiLoadResult> DirectGetAsync(OperationMetadata operationMetadata, string[] keys, string[] includes, string transformer,
                                                           Dictionary<string, RavenJToken> transformerParameters, bool metadataOnly, CancellationToken token = default(CancellationToken))
        {
            var path = operationMetadata.Url + "/queries/?";
            if (metadataOnly)
                path += "&metadata-only=true";
            if (includes != null && includes.Length > 0)
            {
                path += string.Join("&", includes.Select(x => "include=" + x).ToArray());
            }
            if (string.IsNullOrEmpty(transformer) == false)
                path += "&transformer=" + transformer;

            if (transformerParameters != null)
            {
                path = transformerParameters.Aggregate(path,
                                             (current, transformerParam) =>
                                             current + ("&" + string.Format("tp-{0}={1}", transformerParam.Key, transformerParam.Value)));
            }

            var metadata = new RavenJObject();
            AddTransactionInformation(metadata);

            var uniqueIds = new HashSet<string>(keys);
            // if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
            // we are fine with that, requests to load > 128 items are going to be rare
            var isGet = uniqueIds.Sum(x => x.Length) < 1024;
            var method = isGet ? "GET" : "POST";
            if (isGet)
            {
                path += "&" + string.Join("&", uniqueIds.Select(x => "id=" + Uri.EscapeDataString(x)).ToArray());
            }

            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, path, method, metadata, operationMetadata.Credentials, convention)
                .AddOperationHeaders(OperationsHeaders);
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams)
                .AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
            {
                if (isGet == false)
                {
                    await request.WriteAsync(new RavenJArray(uniqueIds)).WithCancellation(token).ConfigureAwait(false);
                }

                var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return await CompleteMultiGetAsync(operationMetadata, keys, includes, transformer, transformerParameters, result, token).ConfigureAwait(false);
            }
        }

        private async Task<MultiLoadResult> CompleteMultiGetAsync(OperationMetadata operationMetadata, string[] keys, string[] includes, string transformer,
                                                           Dictionary<string, RavenJToken> transformerParameters, RavenJToken result, CancellationToken token = default(CancellationToken))
        {
            ErrorResponseException responseException;
            try
            {
                var uniqueKeys = new HashSet<string>(keys);

                var results = result
                    .Value<RavenJArray>("Results")
                    .Select(x => x as RavenJObject)
                    .ToList();

                var documents = results
                    .Where(x => x != null && x.ContainsKey("@metadata") && x["@metadata"].Value<string>("@id") != null)
                    .ToDictionary(x => x["@metadata"].Value<string>("@id"), x => x, StringComparer.OrdinalIgnoreCase);

                if (results.Count >= uniqueKeys.Count)
                {
                    for (var i = 0; i < uniqueKeys.Count; i++)
                    {
                        var key = keys[i];
                        if (documents.ContainsKey(key))
                            continue;

                        documents.Add(key, results[i]);
                    }
                }

                var multiLoadResult = new MultiLoadResult
                {
                    Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
                    Results = documents.Count == 0 ? results : keys.Select(key => documents.ContainsKey(key) ? documents[key] : null).ToList()
                };

                var docResults = multiLoadResult.Results.Concat(multiLoadResult.Includes);

                return
                    await
                    RetryOperationBecauseOfConflict(operationMetadata, docResults, multiLoadResult,
                                                    () => DirectGetAsync(operationMetadata, keys, includes, transformer, transformerParameters, false, token), token: token).ConfigureAwait(false);
            }
            catch (ErrorResponseException e)
            {
                if (e.StatusCode != HttpStatusCode.Conflict)
                    throw;
                responseException = e;
            }
            throw FetchConcurrencyException(responseException);
        }

        public Task<JsonDocument[]> GetDocumentsAsync(int start, int pageSize, bool metadataOnly = false, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                var result = await GetDocumentsInternalAsync(start, null, pageSize, operationMetadata, metadataOnly, token).ConfigureAwait(false);

                return result.Cast<RavenJObject>()
                             .ToJsonDocuments()
                             .ToArray();
            }, token);
        }

        public Task<JsonDocument[]> GetDocumentsAsync(Etag fromEtag, int pageSize, bool metadataOnly = false, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                var result = await GetDocumentsInternalAsync(null, fromEtag, pageSize, operationMetadata, metadataOnly, token).ConfigureAwait(false);
                return result.Cast<RavenJObject>()
                             .ToJsonDocuments()
                             .ToArray();
            }, token);
        }

        public async Task<RavenJArray> GetDocumentsInternalAsync(int? start, Etag fromEtag, int pageSize, OperationMetadata operationMetadata, bool metadataOnly = false, CancellationToken token = default(CancellationToken))
        {
            var requestUri = url + "/docs/?";
            if (start.HasValue && start.Value > 0)
            {
                requestUri += "start=" + start;
            }
            else if (fromEtag != null)
            {
                requestUri += "etag=" + fromEtag;
            }
            requestUri += "&pageSize=" + pageSize;
            if (metadataOnly)
                requestUri += "&metadata-only=true";
            var @params = new CreateHttpJsonRequestParams(this, requestUri, "GET", operationMetadata.Credentials, convention)
                .AddOperationHeaders(OperationsHeaders);

            using (var request = jsonRequestFactory.CreateHttpJsonRequest(@params))
            {
                return (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
            }
        }

        public Task<Operation> UpdateByIndexAsync(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, BulkOperationOptions options = null, CancellationToken token = default(CancellationToken))
        {
            var notNullOptions = options ?? new BulkOperationOptions();
            var requestData = RavenJObject.FromObject(patch).ToString(Formatting.Indented);
            return UpdateByIndexImpl(indexName, queryToUpdate, notNullOptions, requestData, "EVAL", token);
        }

        public Task<Operation> UpdateByIndexAsync(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests,
                BulkOperationOptions options = null, CancellationToken token = default(CancellationToken))
        {
            var notNullOptions = options ?? new BulkOperationOptions();
            var requestData = new RavenJArray(patchRequests.Select(x => x.ToJson())).ToString(Formatting.Indented);
            return UpdateByIndexImpl(indexName, queryToUpdate, notNullOptions, requestData, "PATCH", token);
        }

        public async Task<MultiLoadResult> MoreLikeThisAsync(MoreLikeThisQuery query, CancellationToken token = default(CancellationToken))
        {
            var requestUrl = query.GetRequestUri();
            EnsureIsNotNullOrEmpty(requestUrl, "url");
            var result = await ExecuteWithReplication("GET", async operationMetadata =>
            {
                var metadata = new RavenJObject();
                AddTransactionInformation(metadata);
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + requestUrl, "GET", metadata, operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
                {
                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token).ConfigureAwait(false);
            return ((RavenJObject)result).Deserialize<MultiLoadResult>(convention);
        }

        public Task<long> NextIdentityForAsync(string name, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("POST", async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/identity/next?name=" + Uri.EscapeDataString(name), "POST", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
                {
                    var readResponseJson = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return readResponseJson.Value<long>("Value");
                }
            }, token);
        }

        public Task<long> SeedIdentityForAsync(string name, long value, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("POST", async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/identity/seed?name=" + Uri.EscapeDataString(name) + "&value=" + Uri.EscapeDataString(value.ToString()), "POST", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
                {
                    var readResponseJson = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);

                    return readResponseJson.Value<long>("Value");
                }
            }, token);
        }


        public Task SeedIdentitiesAsync(List<KeyValuePair<string, long>> identities, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("POST", async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/identity/seed/bulk", "POST", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
                {
                    await request.WriteAsync(RavenJToken.FromObject(identities)).ConfigureAwait(false);
                    await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        private Task<Operation> UpdateByIndexImpl(string indexName, IndexQuery queryToUpdate, BulkOperationOptions options, String requestData, String method, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(method, async operationMetadata =>
            {
                var notNullOptions = options ?? new BulkOperationOptions();
                string path = queryToUpdate.GetIndexQueryUrl(operationMetadata.Url, indexName, "bulk_docs") + "&allowStale=" + notNullOptions.AllowStale
                    + "&maxOpsPerSec=" + notNullOptions.MaxOpsPerSec + "&details=" + notNullOptions.RetrieveDetails;
                if (notNullOptions.StaleTimeout != null)
                    path += "&staleTimeout=" + notNullOptions.StaleTimeout;
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path, method, operationMetadata.Credentials, convention)))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    await request.WriteAsync(requestData).ConfigureAwait(false);

                    RavenJToken jsonResponse;
                    try
                    {
                        jsonResponse = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode == HttpStatusCode.NotFound) throw new InvalidOperationException("There is no index named: " + indexName);
                        throw;
                    }

                    return new Operation(this, jsonResponse.Value<long>("OperationId"));
                }
            }, token);
        }

        public Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, string facetSetupDoc, int start = 0,
                                                 int? pageSize = null, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                var requestUri = operationMetadata.Url + string.Format("/facets/{0}?facetDoc={1}{2}&facetStart={3}&facetPageSize={4}",
                Uri.EscapeUriString(index),
                Uri.EscapeDataString(facetSetupDoc),
                query.GetMinimalQueryString(),
                start,
                pageSize);

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, "GET", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    var cachedRequestDetails = jsonRequestFactory.ConfigureCaching(requestUri, (key, val) => request.AddHeader(key, val));
                    request.CachedRequestDetails = cachedRequestDetails.CachedRequest;
                    request.SkipServerCheck = cachedRequestDetails.SkipServerCheck;

                    var json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return json.JsonDeserialization<FacetResults>();
                }
            }, token);
        }

        public Task<FacetResults[]> GetMultiFacetsAsync(FacetQuery[] facetedQueries, CancellationToken token = default(CancellationToken))
        {
            var multiGetReuestItems = facetedQueries.Select(x =>
            {
                string addition;
                if (x.FacetSetupDoc != null)
                {
                    addition = "facetDoc=" + x.FacetSetupDoc;
                    return new GetRequest()
                    {
                        Url = "/facets/" + x.IndexName,
                        Query = string.Format("{0}&facetStart={1}&facetPageSize={2}&{3}",
                            x.Query.GetQueryString(),
                            x.PageStart,
                            x.PageSize,
                            addition)
                    };
                }

                var serializedFacets = SerializeFacetsToFacetsJsonString(x.Facets);
                if (serializedFacets.Length < (32 * 1024) - 1)
                {
                    addition = "facets=" + Uri.EscapeDataString(serializedFacets);
                    return new GetRequest()
                    {
                        Url = "/facets/" + x.IndexName,
                        Query = string.Format("{0}&facetStart={1}&facetPageSize={2}&{3}",
                            x.Query.GetQueryString(),
                            x.PageStart,
                            x.PageSize,
                            addition)
                    };
                }

                return new GetRequest()
                {
                    Url = "/facets/" + x.IndexName,
                    Query = string.Format("{0}&facetStart={1}&facetPageSize={2}",
                            x.Query.GetQueryString(),
                            x.PageStart,
                            x.PageSize),
                    Method = "POST",
                    Content = serializedFacets
                };
            }).ToArray();

            var results = MultiGetAsync(multiGetReuestItems, token).ContinueWith(x =>
            {
                var facetResults = new FacetResults[x.Result.Length];

                var getResponses = x.Result;
                for (var facetResultCounter = 0; facetResultCounter < facetResults.Length; facetResultCounter++)
                {
                    var getResponse = getResponses[facetResultCounter];
                    if (getResponse.RequestHasErrors())
                    {
                        throw new InvalidOperationException("Got an error from server, status code: " + getResponse.Status +
                                                       Environment.NewLine + getResponse.Result);

                    }
                    var curFacetDoc = getResponse.Result;

                    facetResults[facetResultCounter] = curFacetDoc.JsonDeserialization<FacetResults>();
                }

                return facetResults;
            }, token);
            return results;
        }

        public Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, List<Facet> facets, int start = 0,
                                                 int? pageSize = null,
                                                 CancellationToken token = default(CancellationToken))
        {
            var facetsJson = SerializeFacetsToFacetsJsonString(facets);
            var method = facetsJson.Length > 1024 ? "POST" : "GET";
            if (method == "POST")
            {
                return GetMultiFacetsAsync(new[]
                {
                    new FacetQuery
                    {
                        Facets =  facets,
                        IndexName = index,
                        Query = query,
                        PageSize = pageSize,
                        PageStart = start
                    }
                }).ContinueWith(x => x.Result.FirstOrDefault());
            }
            return ExecuteWithReplication(method, async operationMetadata =>
            {
                var requestUri = operationMetadata.Url + string.Format("/facets/{0}?{1}&facetStart={2}&facetPageSize={3}",
                                                                Uri.EscapeUriString(index),
                                                                query.GetQueryString(),
                                                                start,
                                                                pageSize);

                if (method == "GET")
                    requestUri += "&facets=" + Uri.EscapeDataString(facetsJson);
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, method, operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)).AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
                {
                    if (method != "GET")
                    {
                        await request.WriteAsync(facetsJson).ConfigureAwait(false);
                    }

                    var json = await request.ReadResponseJsonAsync().ConfigureAwait(false);

                    return json.JsonDeserialization<FacetResults>();
                }
            });
        }

        internal static string SerializeFacetsToFacetsJsonString(List<Facet> facets)
        {
            var ravenJArray = (RavenJArray)RavenJToken.FromObject(facets, new JsonSerializer
            {
                NullValueHandling = NullValueHandling.Ignore,
                DefaultValueHandling = DefaultValueHandling.Ignore,
            });
            foreach (var facet in ravenJArray)
            {
                var obj = (RavenJObject)facet;
                if (obj.Value<string>("Name") == obj.Value<string>("DisplayName"))
                    obj.Remove("DisplayName");
                var jArray = obj.Value<RavenJArray>("Ranges");
                if (jArray != null && jArray.Length == 0)
                    obj.Remove("Ranges");
            }
            string facetsJson = ravenJArray.ToString(Formatting.None);
            return facetsJson;
        }

        public Task<LogItem[]> GetLogsAsync(bool errorsOnly, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                var requestUri = url + "/logs";
                if (errorsOnly)
                    requestUri += "?type=error";

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, "GET", operationMetadata.Credentials, convention)))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return convention.CreateSerializer().Deserialize<LogItem[]>(new RavenJTokenReader(result));
                }
            }, token);
        }

        public async Task<LicensingStatus> GetLicenseStatusAsync(CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (MultiDatabase.GetRootDatabaseUrl(url) + "/license/status"), "GET", credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention)))
            {
                request.AddOperationHeaders(OperationsHeaders);

                var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return convention.CreateSerializer().Deserialize<LicensingStatus>(new RavenJTokenReader(result));
            }
        }

        public async Task<BuildNumber> GetBuildNumberAsync(CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (url + "/build/version"), "GET", credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention)))
            {
                request.AddOperationHeaders(OperationsHeaders);

                var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return convention.CreateSerializer().Deserialize<BuildNumber>(new RavenJTokenReader(result));
            }
        }

        public async Task<IndexMergeResults> GetIndexMergeSuggestionsAsync(CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (url + "/debug/suggest-index-merge"), "GET", credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention)))
            {
                request.AddOperationHeaders(OperationsHeaders);

                var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return convention.CreateSerializer().Deserialize<IndexMergeResults>(new RavenJTokenReader(result));
            }
        }

        public Task<JsonDocument[]> StartsWithAsync(string keyPrefix, string matches, int start, int pageSize,
                                    RavenPagingInformation pagingInformation = null,
                                    bool metadataOnly = false,
                                    string exclude = null,
                                    string transformer = null,
                                    Dictionary<string, RavenJToken> transformerParameters = null,
                                    string skipAfter = null, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                var metadata = new RavenJObject();
                AddTransactionInformation(metadata);

                var actualStart = start;

                var nextPage = pagingInformation != null && pagingInformation.IsForPreviousPage(start, pageSize);
                if (nextPage)
                    actualStart = pagingInformation.NextPageStart;

                var actualUrl = string.Format("{0}/docs?startsWith={1}&matches={5}&exclude={4}&start={2}&pageSize={3}", operationMetadata.Url,
                                              Uri.EscapeDataString(keyPrefix), actualStart.ToInvariantString(), pageSize.ToInvariantString(), exclude, matches);

                if (metadataOnly)
                    actualUrl += "&metadata-only=true";

                if (string.IsNullOrEmpty(skipAfter) == false)
                    actualUrl += "&skipAfter=" + Uri.EscapeDataString(skipAfter);

                if (string.IsNullOrEmpty(transformer) == false)
                {
                    actualUrl += "&transformer=" + transformer;

                    if (transformerParameters != null)
                    {
                        actualUrl = transformerParameters.Aggregate(actualUrl,
                                             (current, transformerParamater) =>
                                             current + ("&" + string.Format("tp-{0}={1}", transformerParamater.Key, transformerParamater.Value)));
                    }
                }

                if (nextPage)
                    actualUrl += "&next-page=true";

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, actualUrl, "GET", metadata, operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    var result = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);

                    int nextPageStart;
                    if (pagingInformation != null && int.TryParse(request.ResponseHeaders[Constants.NextPageStart], out nextPageStart)) pagingInformation.Fill(start, pageSize, nextPageStart);

                    var docResults = result.OfType<RavenJObject>().ToList();
                    var startsWithResults = SerializationHelper.RavenJObjectsToJsonDocuments(docResults.Select(x => (RavenJObject)x.CloneToken())).ToArray();
                    return await RetryOperationBecauseOfConflict(operationMetadata, docResults, startsWithResults, () =>
                        StartsWithAsync(keyPrefix, matches, start, pageSize, pagingInformation, metadataOnly, exclude, transformer, transformerParameters, skipAfter, token), conflictedResultId =>
                            new ConflictException("Conflict detected on " + conflictedResultId.Substring(0, conflictedResultId.IndexOf("/conflicts/", StringComparison.OrdinalIgnoreCase)) +
                                ", conflict must be resolved before the document will be accessible", true)
                            { ConflictedVersionIds = new[] { conflictedResultId } }, token).ConfigureAwait(false);
                }
            }, token);
        }

        public Task<GetResponse[]> MultiGetAsync(GetRequest[] requests, CancellationToken token = default(CancellationToken))
        {
            return MultiGetAsyncInternal(requests, token, null);
        }

        private Task<GetResponse[]> MultiGetAsyncInternal(GetRequest[] requests, CancellationToken token, Reference<OperationMetadata> operationMetadataRef)
        {
            return ExecuteWithReplication<GetResponse[]>("GET", async operationMetadata => // logical GET even though the actual request is a POST
            {
                if (operationMetadataRef != null)
                    operationMetadataRef.Value = operationMetadata;
                var multiGetOperation = new MultiGetOperation(this, convention, operationMetadata.Url, requests);

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, multiGetOperation.RequestUri, "POST", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    var requestsForServer = multiGetOperation.PreparingForCachingRequest(jsonRequestFactory);

                    var postedData = JsonConvert.SerializeObject(requestsForServer);

                    if (multiGetOperation.CanFullyCache(jsonRequestFactory, request, postedData))
                    {
                        var cachedResponses = multiGetOperation.HandleCachingResponse(new GetResponse[requests.Length], jsonRequestFactory);
                        return cachedResponses;
                    }

                    await request.WriteAsync(postedData).ConfigureAwait(false);
                    var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    var responses = convention.CreateSerializer().Deserialize<GetResponse[]>(new RavenJTokenReader(result));

                    await multiGetOperation.TryResolveConflictOrCreateConcurrencyException(responses, (key, conflictDoc, etag) => TryResolveConflictOrCreateConcurrencyException(operationMetadata, key, conflictDoc, etag, token)).ConfigureAwait(false);

                    return multiGetOperation.HandleCachingResponse(responses, jsonRequestFactory);
                }
            }, token);
        }

        public Task<QueryResult> QueryAsync(string index, IndexQuery query, string[] includes = null, bool metadataOnly = false, bool indexEntriesOnly = false, CancellationToken token = default(CancellationToken))
        {
            var method = (query.Query == null || query.Query.Length <= convention.MaxLengthOfQueryUsingGetUrl)
                ? "GET" : "POST";

            if (method == "POST")
            {
                return QueryAsyncAsPost(index, query, includes, metadataOnly, indexEntriesOnly, token);
            }

            return QueryAsyncAsGet(index, query, includes, metadataOnly, indexEntriesOnly, method, token);
        }

        private Task<QueryResult> QueryAsyncAsGet(string index, IndexQuery query, string[] includes, bool metadataOnly, bool indexEntriesOnly, string method, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(method, async operationMetadata =>
            {
                EnsureIsNotNullOrEmpty(index, "index");
                string path = query.GetIndexQueryUrl(operationMetadata.Url, index, "indexes", includeQuery: method == "GET");

                if (metadataOnly)
                    path += "&metadata-only=true";
                if (indexEntriesOnly)
                    path += "&debug=entries";
                if (includes != null && includes.Length > 0)
                {
                    path += "&" + string.Join("&", includes.Select(x => "include=" + x).ToArray());
                }

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path, method, operationMetadata.Credentials, convention) { AvoidCachingRequest = query.DisableCaching }.AddOperationHeaders(OperationsHeaders)))
                {
                    RavenJObject json = null;
                    request.AddReplicationStatusHeaders(operationMetadata.Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);


                    ErrorResponseException responseException;
                    try
                    {
                        if (json == null) throw new InvalidOperationException("Got empty response from the server for the following request: " + request.Url);

                        var queryResult = SerializationHelper.ToQueryResult(json, request.ResponseHeaders.GetEtagHeader(), request.ResponseHeaders.Get("Temp-Request-Time"), request.Size);

                        if (request.ResponseStatusCode == HttpStatusCode.NotModified)
                            queryResult.DurationMilliseconds = -1;

                        var docResults = queryResult.Results.Concat(queryResult.Includes);
                        return await RetryOperationBecauseOfConflict(operationMetadata, docResults, queryResult,
                            () => QueryAsync(index, query, includes, metadataOnly, indexEntriesOnly, token),
                            conflictedResultId => new ConflictException("Conflict detected on " + conflictedResultId.Substring(0, conflictedResultId.IndexOf("/conflicts/", StringComparison.OrdinalIgnoreCase)) +
                                    ", conflict must be resolved before the document will be accessible", true)
                            { ConflictedVersionIds = new[] { conflictedResultId } },
                            token).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode == HttpStatusCode.NotFound)
                        {
                            var text = e.ResponseString;
                            if (text.Contains("maxQueryString")) throw new ErrorResponseException(e, text);
                            throw new ErrorResponseException(e, "There is no index named: " + index);
                        }
                        responseException = e;
                    }
                    if (HandleException(responseException)) return null;
                    throw responseException;
                }
            }, token);
        }

        private async Task<QueryResult> QueryAsyncAsPost(string index, IndexQuery query, string[] includes, bool metadataOnly, bool indexEntriesOnly, CancellationToken token = default(CancellationToken))
        {
            var stringBuilder = new StringBuilder();
            query.AppendQueryString(stringBuilder);

            if (metadataOnly)
                stringBuilder.Append("&metadata-only=true");
            if (indexEntriesOnly)
                stringBuilder.Append("&debug=entries");
            if (includes != null && includes.Length > 0)
            {
                includes.ForEach(include => stringBuilder.Append("&include=").Append(include));
            }

            try
            {
                var operationMetadataRef = new Reference<OperationMetadata>();
                var result = await MultiGetAsyncInternal(new[]
                {
                    new GetRequest
                    {
                        Query = stringBuilder.ToString(),
                        Url = "/indexes/" + index
                    }
                }, token, operationMetadataRef).ConfigureAwait(false);

                var json = (RavenJObject)result[0].Result;
                var queryResult = SerializationHelper.ToQueryResult(json, result[0].GetEtagHeader(), result[0].Headers["Temp-Request-Time"], -1);

                var docResults = queryResult.Results.Concat(queryResult.Includes);
                return await RetryOperationBecauseOfConflict(operationMetadataRef.Value, docResults, queryResult,
                    () => QueryAsync(index, query, includes, metadataOnly, indexEntriesOnly, token),
                    conflictedResultId => new ConflictException("Conflict detected on " + conflictedResultId.Substring(0, conflictedResultId.IndexOf("/conflicts/", StringComparison.OrdinalIgnoreCase)) +
                            ", conflict must be resolved before the document will be accessible", true)
                    { ConflictedVersionIds = new[] { conflictedResultId } },
                    token).ConfigureAwait(false);
            }
            catch (OperationCanceledException oce)
            {
                throw new TaskCanceledException(string.Format("Canceled Index {0} Query", index), oce);
            }
            catch (Exception e)
            {
                var errorResponseException = e as ErrorResponseException;

                if (errorResponseException != null)
                {
                    if (errorResponseException.StatusCode == HttpStatusCode.NotFound)
                    {
                        var text = errorResponseException.ResponseString;
                        if (text.Contains("maxQueryString")) throw new ErrorResponseException(errorResponseException, text);
                        throw new ErrorResponseException(errorResponseException, "There is no index named: " + index);
                    }

                    if (HandleException(errorResponseException)) return null;
                }

                throw;
            }
        }

        /// <summary>
        /// Attempts to handle an exception raised when receiving a response from the server
        /// </summary>
        /// <param name="e">The exception to handle</param>
        /// <returns>returns true if the exception is handled, false if it should be thrown</returns>
        private bool HandleException(ErrorResponseException e)
        {
            if (e.StatusCode == HttpStatusCode.InternalServerError)
            {
                var content = e.ResponseString;
                var json = RavenJObject.Load(new JsonTextReader(new StringReader(content)));
                var error = json.Deserialize<ServerRequestError>(convention);

                throw new ErrorResponseException(e, error.Error);
            }
            return false;
        }

        public Task<SuggestionQueryResult> SuggestAsync(string index, SuggestionQuery suggestionQuery, CancellationToken token = default(CancellationToken))
        {
            if (suggestionQuery == null)
                throw new ArgumentNullException("suggestionQuery");

            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                var requestUri = operationMetadata.Url + string.Format("/suggest/{0}?term={1}&field={2}&max={3}&popularity={4}",
                    Uri.EscapeUriString(index),
                    Uri.EscapeDataString(suggestionQuery.Term),
                    Uri.EscapeDataString(suggestionQuery.Field),
                    Uri.EscapeDataString(suggestionQuery.MaxSuggestions.ToInvariantString()),
                                                              suggestionQuery.Popularity);

                if (suggestionQuery.Accuracy.HasValue)
                    requestUri += "&accuracy=" + suggestionQuery.Accuracy.Value.ToInvariantString();

                if (suggestionQuery.Distance.HasValue)
                    requestUri += "&distance=" + suggestionQuery.Distance;

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, "GET", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    var json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return new SuggestionQueryResult { Suggestions = ((RavenJArray)json["Suggestions"]).Select(x => x.Value<string>()).ToArray(), };
                }
            }, token);
        }

        public Task<BatchResult[]> BatchAsync(IEnumerable<ICommandData> commandDatas, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("POST", async operationMetadata =>
            {
                var metadata = new RavenJObject();
                AddTransactionInformation(metadata);

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/bulk_docs", "POST", metadata, operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    var serializedData = commandDatas.Select(x => x.ToJson()).ToList();
                    var jArray = new RavenJArray(serializedData);

                    ErrorResponseException responseException;
                    try
                    {
                        await request.WriteAsync(jArray).WithCancellation(token).ConfigureAwait(false);
                        var response = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                        if (response == null)
                        {
                            throw new InvalidOperationException("Got null response from the server after doing a batch, something is very wrong. Probably a garbled response. Posted: " + jArray);
                        }
                        return convention.CreateSerializer().Deserialize<BatchResult[]>(new RavenJTokenReader(response));
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode != HttpStatusCode.Conflict) throw;
                        responseException = e;
                    }
                    throw FetchConcurrencyException(responseException);
                }
            }, token);
        }

        private static ConcurrencyException FetchConcurrencyException(ErrorResponseException e)
        {
            var text = e.ResponseString;
            var errorResults = JsonConvert.DeserializeAnonymousType(text, new
            {
                url = (string)null,
                actualETag = Etag.Empty,
                expectedETag = Etag.Empty,
                error = (string)null
            });
            return new ConcurrencyException(errorResults.error)
            {
                ActualETag = errorResults.actualETag,
                ExpectedETag = errorResults.expectedETag
            };
        }

        private void AddTransactionInformation(RavenJObject metadata)
        {
            if (convention.EnlistInDistributedTransactions == false)
                return;

#if !DNXCORE50
            var transactionInformation = RavenTransactionAccessor.GetTransactionInformation();
            if (transactionInformation == null)
                return;

            string txInfo = string.Format("{0}, {1}", transactionInformation.Id, transactionInformation.Timeout);
            metadata["Raven-Transaction-Information"] = new RavenJValue(txInfo);
#endif
        }

        private static void EnsureIsNotNullOrEmpty(string key, string argName)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key cannot be null or empty", argName);
        }

        public async Task<DatabaseStatistics> GetStatisticsAsync(CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url.Stats(), "GET", credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention)))
            {
                var json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return json.Deserialize<DatabaseStatistics>(convention);
            }
        }

        [Obsolete("Use RavenFS instead.")]
        public Task<AttachmentInformation[]> GetAttachmentsAsync(int start, Etag startEtag, int pageSize, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/static/?pageSize=" + pageSize + "&etag=" + startEtag + "&start=" + start, "GET", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    var json = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return convention.CreateSerializer().Deserialize<AttachmentInformation[]>(new RavenJTokenReader(json));
                }
            }, token);
        }

        [Obsolete("Use RavenFS instead.")]
        public Task PutAttachmentAsync(string key, Etag etag, Stream data, RavenJObject metadata, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("PUT", async operationMetadata =>
            {
                if (metadata == null)
                    metadata = new RavenJObject();

                if (etag != null)
                    metadata[Constants.MetadataEtagField] = new RavenJValue((string)etag);
                else
                    metadata.Remove(Constants.MetadataEtagField);

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Static(operationMetadata.Url, key), "PUT", metadata, operationMetadata.Credentials, convention)))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    await request.WriteAsync(data).WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        [Obsolete("Use RavenFS instead.")]
        public Task<Attachment> GetAttachmentAsync(string key, CancellationToken token = default(CancellationToken))
        {
            EnsureIsNotNullOrEmpty(key, "key");

            return ExecuteWithReplication("GET", operationMetadata => DirectGetAttachmentAsync(key, operationMetadata, "GET", token), token);
        }

        [Obsolete("Use RavenFS instead.")]
        public Task<Attachment> HeadAttachmentAsync(string key, CancellationToken token = default(CancellationToken))
        {
            EnsureIsNotNullOrEmpty(key, "key");

            return ExecuteWithReplication("HEAD", operationMetadata => DirectGetAttachmentAsync(key, operationMetadata, "HEAD", token), token);
        }

        [Obsolete("Use RavenFS instead.")]
        private async Task<Attachment> DirectGetAttachmentAsync(string key, OperationMetadata operationMetadata, string method, CancellationToken token = default(CancellationToken))
        {
            var metadata = new RavenJObject();
            AddTransactionInformation(metadata);
            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, (operationMetadata.Url + "/static/" + key), method, metadata, operationMetadata.Credentials, convention);
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams.AddOperationHeaders(OperationsHeaders)).AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
            {
                ErrorResponseException responseException;
                try
                {
                    var result = await request.ReadResponseBytesAsync().WithCancellation(token).ConfigureAwait(false);
                    HandleReplicationStatusChanges(request.ResponseHeaders, Url, operationMetadata.Url);

                    if (method == "GET")
                    {
                        var memoryStream = new MemoryStream(result);
                        return new Attachment
                        {
                            Key = key,
                            Data = () => memoryStream,
                            Size = result.Length,
                            Etag = request.ResponseHeaders.GetEtagHeader(),
                            Metadata = request.ResponseHeaders.FilterHeadersAttachment()
                        };
                    }
                    else
                    {
                        return new Attachment
                        {
                            Key = key,
                            Data = () =>
                            {
                                throw new InvalidOperationException("Cannot get attachment data because it was loaded using: " + method);
                            },
#if !DNXCORE50
                            Size = int.Parse(request.ResponseHeaders["Content-Length"]),
#else
                            Size = int.Parse(request.ResponseHeaders["Raven-Content-Length"]),
#endif
                            Etag = request.ResponseHeaders.GetEtagHeader(),
                            Metadata = request.ResponseHeaders.FilterHeadersAttachment()
                        };
                    }
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode == HttpStatusCode.NotFound) return null;
                    if (e.StatusCode != HttpStatusCode.Conflict) throw;
                    responseException = e;
                }

                using (var stream = await responseException.Response.GetResponseStreamWithHttpDecompression().WithCancellation(token).ConfigureAwait(false))
                {
                    string[] conflictIds;
                    if (method == "GET")
                    {
                        var conflictsDoc = stream.ToJObject();
                        conflictIds = conflictsDoc.Value<RavenJArray>("Conflicts").Select(x => x.Value<string>()).ToArray();
                    }
                    else
                    {
                        conflictIds = new[] { "Cannot get conflict ids in HEAD requesT" };
                    }

                    throw new ConflictException("Conflict detected on " + key + ", conflict must be resolved before the attachment will be accessible", true) { ConflictedVersionIds = conflictIds, Etag = responseException.Etag };
                }
            }
        }

        [Obsolete("Use RavenFS instead.")]
        public Task DeleteAttachmentAsync(string key, Etag etag, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("DELETE", operationMetadata =>
            {
                var metadata = new RavenJObject();

                if (etag != null)
                    metadata[Constants.MetadataEtagField] = new RavenJValue((string)etag);

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Static(operationMetadata.Url, key), "DELETE", metadata, operationMetadata.Credentials, convention)))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    request.AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

                    return request.ExecuteRequestAsync();
                }
            }, token);
        }

        public static string Static(string url, string key)
        {
            return url + "/static/" + Uri.EscapeUriString(key);
        }

        public IDisposable DisableAllCaching()
        {
            return jsonRequestFactory.DisableAllCaching();
        }

        public Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Terms(index, field, fromValue, pageSize), "GET", operationMetadata.Credentials, convention)).AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer,
                                                                            convention.FailoverBehavior,
                                                                            HandleReplicationStatusChanges))
                {
                    var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    var json = ((RavenJArray)result);
                    return json.Select(x => x.Value<string>()).ToArray();
                }
            }, token);
        }

        public ProfilingInformation ProfilingInformation
        {
            get { return profilingInformation; }
        }

        public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
        {
            add { replicationInformer.FailoverStatusChanged += value; }
            remove { replicationInformer.FailoverStatusChanged -= value; }
        }

        public IDisposable ForceReadFromMaster()
        {
            var old = readStripingBase;
            readStripingBase = -1;// this means that will have to use the master url first
            return new DisposableAction(() => readStripingBase = old);
        }

        public Task<JsonDocumentMetadata> HeadAsync(string key, CancellationToken token = default(CancellationToken))
        {
            EnsureIsNotNullOrEmpty(key, "key");
            return ExecuteWithReplication("HEAD", u => DirectHeadAsync(u, key, token), token);
        }

        public async Task<IAsyncEnumerator<RavenJObject>> StreamExportAsync(ExportOptions options, CancellationToken cancellationToken = default(CancellationToken))
        {
            var path = "/smuggler/export";
            var request = CreateRequest(path, "POST");

            request.RemoveAuthorizationHeader();

            var tokenRetriever = new SingleAuthTokenRetriever(this, jsonRequestFactory, convention, OperationsHeaders, new OperationMetadata(Url, PrimaryCredentials));

            var token = await tokenRetriever.GetToken().WithCancellation(cancellationToken).ConfigureAwait(false);
            try
            {
                token = await tokenRetriever.ValidateThatWeCanUseToken(token).WithCancellation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                request.Dispose();

                throw new InvalidOperationException(
                    "Could not authenticate token for export streaming, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration",
                    e);
            }
            request.AddOperationHeader("Single-Use-Auth-Token", token);

            HttpResponseMessage response;
            try
            {
                response = await request.ExecuteRawResponseAsync(RavenJObject.FromObject(options))
                                        .WithCancellation(cancellationToken)
                                        .ConfigureAwait(false);

                await response.AssertNotFailingResponse().WithCancellation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
                request.Dispose();

                throw;
            }

            return new YieldStreamResults(request, await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false));
        }

        public Task<IAsyncEnumerator<RavenJObject>> StreamQueryAsync(string index, IndexQuery query, Reference<QueryHeaderInformation> queryHeaderInfo, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", operationMetadata => DirectStreamQueryAsync(index, query, queryHeaderInfo, operationMetadata, token), token);
        }

        private async Task<IAsyncEnumerator<RavenJObject>> DirectStreamQueryAsync(string index, IndexQuery query, Reference<QueryHeaderInformation> queryHeaderInfo, OperationMetadata operationMetadata, CancellationToken cancellationToken = default(CancellationToken))
        {
            EnsureIsNotNullOrEmpty(index, "index");
            string path;
            string method;
            if (query.Query != null && query.Query.Length > convention.MaxLengthOfQueryUsingGetUrl)
            {
                path = query.GetIndexQueryUrl(operationMetadata.Url, index, "streams/query", includePageSizeEvenIfNotExplicitlySet: false, includeQuery: false);
                method = "POST";
            }
            else
            {
                method = "GET";
                path = query.GetIndexQueryUrl(operationMetadata.Url, index, "streams/query", includePageSizeEvenIfNotExplicitlySet: false);
            }

            var request = jsonRequestFactory
                .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path, method, operationMetadata.Credentials, convention)
                .AddOperationHeaders(OperationsHeaders))
                .AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

            request.RemoveAuthorizationHeader();

            var tokenRetriever = new SingleAuthTokenRetriever(this, jsonRequestFactory, convention, OperationsHeaders, operationMetadata);

            var token = await tokenRetriever.GetToken().WithCancellation(cancellationToken).ConfigureAwait(false);
            try
            {
                token = await tokenRetriever.ValidateThatWeCanUseToken(token).WithCancellation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                request.Dispose();

                throw new InvalidOperationException(
                    "Could not authenticate token for query streaming, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration",
                    e);
            }
            request.AddOperationHeader("Single-Use-Auth-Token", token);

            HttpResponseMessage response;
            try
            {
                if (method == "POST")
                {
                    response = await request.ExecuteRawResponseAsync(query.Query)
                                            .WithCancellation(cancellationToken)
                                            .ConfigureAwait(false);
                }
                else
                {
                    response = await request.ExecuteRawResponseAsync()
                                            .WithCancellation(cancellationToken)
                                            .ConfigureAwait(false);
                }

                await response.AssertNotFailingResponse().WithCancellation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                request.Dispose();

                if (index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) && request.ResponseStatusCode == HttpStatusCode.NotFound)
                {
                    throw new InvalidOperationException(
                        @"StreamQuery does not support querying dynamic indexes. It is designed to be used with large data-sets and is unlikely to return all data-set after 15 sec of indexing, like Query() does.",
                        e);
                }

                throw;
            }

            queryHeaderInfo.Value = new QueryHeaderInformation
            {
                Index = response.Headers.GetFirstValue("Raven-Index"),
                IndexTimestamp = DateTime.ParseExact(response.Headers.GetFirstValue("Raven-Index-Timestamp"), Default.DateTimeFormatsToRead,
                                                                CultureInfo.InvariantCulture, DateTimeStyles.None),
                IndexEtag = Etag.Parse(response.Headers.GetFirstValue("Raven-Index-Etag")),
                ResultEtag = Etag.Parse(response.Headers.GetFirstValue("Raven-Result-Etag")),
                IsStale = bool.Parse(response.Headers.GetFirstValue("Raven-Is-Stale")),
                TotalResults = int.Parse(response.Headers.GetFirstValue("Raven-Total-Results"))
            };

            return new YieldStreamResults(request, await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false));
        }

        public class YieldStreamResults : IAsyncEnumerator<RavenJObject>
        {
            private readonly HttpJsonRequest request;

            private readonly int start;

            private readonly int pageSize;

            private readonly RavenPagingInformation pagingInformation;

            private readonly Stream stream;
            private readonly StreamReader streamReader;
            private readonly JsonTextReaderAsync reader;
            private bool complete;

            private bool wasInitialized;
            private readonly Func<JsonTextReaderAsync, bool> customizedEndResult;

            public YieldStreamResults(HttpJsonRequest request, Stream stream, int start = 0, int pageSize = 0, RavenPagingInformation pagingInformation = null, Func<JsonTextReaderAsync, bool> customizedEndResult = null)
            {
                this.request = request;
                this.start = start;
                this.pageSize = pageSize;
                this.pagingInformation = pagingInformation;
                this.stream = stream;
                this.customizedEndResult = customizedEndResult;
                streamReader = new StreamReader(stream);
                reader = new JsonTextReaderAsync(streamReader);
            }

            private async Task InitAsync()
            {
                if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType != JsonToken.StartObject)
                    throw new InvalidOperationException("Unexpected data at start of stream");

                if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType != JsonToken.PropertyName || Equals("Results", reader.Value) == false)
                    throw new InvalidOperationException("Unexpected data at stream 'Results' property name");

                if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType != JsonToken.StartArray)
                    throw new InvalidOperationException("Unexpected data at 'Results', could not find start results array");
            }

            public void Dispose()
            {
                try
                {
                    reader.Close();
                }
                catch (Exception)
                {
                }

                try
                {
#if !DNXCORE50
                    streamReader.Close();
#else
                    streamReader.Dispose();
#endif
                }
                catch (Exception)
                {
                }

                try
                {
#if !DNXCORE50
                    stream.Close();
#else
                    stream.Dispose();
#endif
                }
                catch (Exception)
                {
                }

                try
                {
                    request.Dispose();
                }
                catch (Exception)
                {
                }
            }

            public async Task<bool> MoveNextAsync()
            {
                if (complete)
                {
                    // to parallel IEnumerable<T>, subsequent calls to MoveNextAsync after it has returned false should
                    // also return false, rather than throwing
                    return false;
                }

                if (wasInitialized == false)
                {
                    await InitAsync().ConfigureAwait(false);
                    wasInitialized = true;
                }

                if (await reader.ReadAsync().ConfigureAwait(false) == false)
                    throw new InvalidOperationException("Unexpected end of data");

                if (reader.TokenType == JsonToken.EndArray)
                {
                    complete = true;

                    await TryReadNextPageStart().ConfigureAwait(false);

                    await EnsureValidEndOfResponse().ConfigureAwait(false);
                    this.Dispose();
                    return false;
                }
                Current = (RavenJObject)await RavenJToken.ReadFromAsync(reader).ConfigureAwait(false);
                return true;
            }

            private async Task TryReadNextPageStart()
            {
                if (!(await reader.ReadAsync().ConfigureAwait(false)) || reader.TokenType != JsonToken.PropertyName)
                    return;

                switch ((string)reader.Value)
                {
                    case "NextPageStart":
                        var nextPageStart = await reader.ReadAsInt32().ConfigureAwait(false);
                        if (pagingInformation == null)
                            return;
                        if (nextPageStart.HasValue == false)
                            throw new InvalidOperationException("Unexpected end of data");

                        pagingInformation.Fill(start, pageSize, nextPageStart.Value);
                        break;
                    case "Error":
                        var err = await reader.ReadAsString().ConfigureAwait(false);
                        throw new InvalidOperationException("Server error" + Environment.NewLine + err);
                    default:
                        if (customizedEndResult != null && customizedEndResult(reader))
                            break;

                        throw new InvalidOperationException("Unexpected property name: " + reader.Value);
                }

            }

            private async Task EnsureValidEndOfResponse()
            {
                if (reader.TokenType != JsonToken.EndObject && await reader.ReadAsync().ConfigureAwait(false) == false)
                    throw new InvalidOperationException("Unexpected end of response - missing EndObject token");

                if (reader.TokenType != JsonToken.EndObject)
                    throw new InvalidOperationException(string.Format("Unexpected token type at the end of the response: {0}. Error: {1}", reader.TokenType, streamReader.ReadToEnd()));

                var remainingContent = await streamReader.ReadToEndAsync().ConfigureAwait(false);

                if (string.IsNullOrEmpty(remainingContent) == false)
                    throw new InvalidOperationException("Server error: " + remainingContent);
            }

            public RavenJObject Current { get; private set; }
        }

        public async Task<IAsyncEnumerator<RavenJObject>> StreamDocsAsync(
                        Etag fromEtag = null, string startsWith = null,
                        string matches = null, int start = 0,
                        int pageSize = Int32.MaxValue,
                        string exclude = null,
                        RavenPagingInformation pagingInformation = null,
                        string skipAfter = null, CancellationToken token = default(CancellationToken))
        {
            if (fromEtag != null && startsWith != null)
                throw new InvalidOperationException("Either fromEtag or startsWith must be null, you can't specify both");

            if (fromEtag != null) // etags does not match between servers
                return await DirectStreamDocsAsync(fromEtag, null, matches, start, pageSize, exclude, pagingInformation, new OperationMetadata(url, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication), skipAfter, token).ConfigureAwait(false);

            return await ExecuteWithReplication("GET", operationMetadata => DirectStreamDocsAsync(null, startsWith, matches, start, pageSize, exclude, pagingInformation, operationMetadata, skipAfter, token), token).ConfigureAwait(false);
        }

        private async Task<IAsyncEnumerator<RavenJObject>> DirectStreamDocsAsync(Etag fromEtag, string startsWith, string matches, int start, int pageSize, string exclude, RavenPagingInformation pagingInformation, OperationMetadata operationMetadata, string skipAfter, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (fromEtag != null && startsWith != null)
                throw new InvalidOperationException("Either fromEtag or startsWith must be null, you can't specify both");

            var sb = new StringBuilder(operationMetadata.Url).Append("/streams/docs?");

            if (fromEtag != null)
            {
                sb.Append("etag=").Append(fromEtag).Append("&");
            }
            else
            {
                if (startsWith != null)
                {
                    sb.Append("startsWith=").Append(Uri.EscapeDataString(startsWith)).Append("&");
                }
                if (matches != null)
                {
                    sb.Append("matches=").Append(Uri.EscapeDataString(matches)).Append("&");
                }
                if (exclude != null)
                {
                    sb.Append("exclude=").Append(Uri.EscapeDataString(exclude)).Append("&");
                }
                if (skipAfter != null)
                {
                    sb.Append("skipAfter=").Append(Uri.EscapeDataString(skipAfter)).Append("&");
                }
            }

            var actualStart = start;

            var nextPage = pagingInformation != null && pagingInformation.IsForPreviousPage(start, pageSize);
            if (nextPage)
                actualStart = pagingInformation.NextPageStart;

            if (actualStart != 0)
                sb.Append("start=").Append(actualStart).Append("&");

            if (pageSize != int.MaxValue)
                sb.Append("pageSize=").Append(pageSize).Append("&");

            if (nextPage)
                sb.Append("next-page=true").Append("&");

            var request = jsonRequestFactory
                .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, sb.ToString(), "GET", operationMetadata.Credentials, convention)
                .AddOperationHeaders(OperationsHeaders))
                .AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);

            request.RemoveAuthorizationHeader();

            var tokenRetriever = new SingleAuthTokenRetriever(this, jsonRequestFactory, convention, OperationsHeaders, operationMetadata);

            var token = await tokenRetriever.GetToken().WithCancellation(cancellationToken).ConfigureAwait(false);
            try
            {
                token = await tokenRetriever.ValidateThatWeCanUseToken(token).WithCancellation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                request.Dispose();

                throw new InvalidOperationException(
                    "Could not authenticate token for query streaming, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration",
                    e);
            }
            request.AddOperationHeader("Single-Use-Auth-Token", token);

            HttpResponseMessage response;

            try
            {
                response = await request.ExecuteRawResponseAsync()
                                        .WithCancellation(cancellationToken)
                                        .ConfigureAwait(false);

                await response.AssertNotFailingResponse().WithCancellation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
                request.Dispose();

                throw;
            }

            return new YieldStreamResults(request, await response.GetResponseStreamWithHttpDecompression().WithCancellation(cancellationToken).ConfigureAwait(false), start, pageSize, pagingInformation);
        }

        public Task DeleteAsync(string key, Etag etag, CancellationToken token = default(CancellationToken))
        {
            EnsureIsNotNullOrEmpty(key, "key");
            return ExecuteWithReplication("DELETE", async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Doc(key), "DELETE", operationMetadata.Credentials, convention).AddOperationHeaders(operationsHeaders)))
                {
                    request.AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges);
                    if (etag != null)
                        request.AddHeader("If-None-Match", etag);

                    try
                    {
                        await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode != HttpStatusCode.Conflict)
                            throw;

                        throw FetchConcurrencyException(e);
                    }
                }
            }, token);
        }

        public string UrlFor(string documentKey)
        {
            return url + "/docs/" + documentKey;
        }

        public ILowLevelBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options, IDatabaseChanges changes)
        {
            if (options.ChunkedBulkInsertOptions != null)
                return new ChunkedRemoteBulkInsertOperation(options, this, changes);
            return new RemoteBulkInsertOperation(options, this, changes);
        }

        private async Task<JsonDocumentMetadata> DirectHeadAsync(OperationMetadata operationMetadata, string key, CancellationToken token = default(CancellationToken))
        {
            var metadata = new RavenJObject();
            AddTransactionInformation(metadata);
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/docs/" + key, "HEAD", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)).AddReplicationStatusHeaders(Url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
            {
                try
                {
                    await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return SerializationHelper.DeserializeJsonDocumentMetadata(key, request.ResponseHeaders, request.ResponseStatusCode);
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode == HttpStatusCode.NotFound) return null;
                    if (e.StatusCode == HttpStatusCode.Conflict)
                    {
                        throw new ConflictException("Conflict detected on " + key + ", conflict must be resolved before the document will be accessible. Cannot get the conflicts ids because a HEAD request was performed. A GET request will provide more information, and if you have a document conflict listener, will automatically resolve the conflict", true) { Etag = e.Etag };
                    }
                    throw;
                }
            }
        }

        public Task<RavenJToken> ExecuteGetRequest(string requestUrl)
        {
            EnsureIsNotNullOrEmpty(requestUrl, "url");
            return ExecuteWithReplication("GET", async operationMetadata =>
            {
                var metadata = new RavenJObject();
                AddTransactionInformation(metadata);
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + requestUrl, "GET", metadata, operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
                {
                    return await request.ReadResponseJsonAsync().ConfigureAwait(false);
                }
            });
        }

        public HttpJsonRequest CreateRequest(string requestUrl, string method, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
        {
            var metadata = new RavenJObject();
            AddTransactionInformation(metadata);
            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, url + requestUrl, method, metadata, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention, timeout)
                .AddOperationHeaders(OperationsHeaders);
            createHttpJsonRequestParams.DisableRequestCompression = disableRequestCompression;
            createHttpJsonRequestParams.DisableAuthentication = disableAuthentication;
            return jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams);
        }

        public HttpJsonRequest CreateReplicationAwareRequest(string currentServerUrl, string requestUrl, string method, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
        {
            var metadata = new RavenJObject();
            AddTransactionInformation(metadata);

            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, currentServerUrl + requestUrl, method, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication,
                                                                              convention, timeout).AddOperationHeaders(OperationsHeaders);
            createHttpJsonRequestParams.DisableRequestCompression = disableRequestCompression;
            createHttpJsonRequestParams.DisableAuthentication = disableAuthentication;

            return jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams)
                                     .AddReplicationStatusHeaders(url, currentServerUrl, replicationInformer,
                                                                  convention.FailoverBehavior, HandleReplicationStatusChanges);
        }

        [Obsolete("Use RavenFS instead.")]
        public Task UpdateAttachmentMetadataAsync(string key, Etag etag, RavenJObject metadata, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("POST", operationMetadata => DirectUpdateAttachmentMetadata(key, metadata, etag, operationMetadata, token), token);
        }

        [Obsolete("Use RavenFS instead.")]
        private async Task DirectUpdateAttachmentMetadata(string key, RavenJObject metadata, Etag etag, OperationMetadata operationMetadata, CancellationToken token = default(CancellationToken))
        {
            if (etag != null)
            {
                metadata[Constants.MetadataEtagField] = etag.ToString();
            }
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/static/" + key, "POST", metadata, operationMetadata.Credentials, convention)).AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
            {
                ErrorResponseException responseException;
                try
                {
                    await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                    return;
                }
                catch (ErrorResponseException e)
                {
                    responseException = e;
                }
                if (!HandleException(responseException)) throw responseException;
            }
        }

        [Obsolete("Use RavenFS instead.")]
        public Task<IAsyncEnumerator<Attachment>> GetAttachmentHeadersStartingWithAsync(string idPrefix, int start, int pageSize, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("GET", operationMetadata => DirectGetAttachmentHeadersStartingWith("GET", idPrefix, start, pageSize, operationMetadata, token), token);
        }

        [Obsolete("Use RavenFS instead.")]
        private async Task<IAsyncEnumerator<Attachment>> DirectGetAttachmentHeadersStartingWith(string method, string idPrefix, int start, int pageSize, OperationMetadata operationMetadata, CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/static/?startsWith=" + idPrefix + "&start=" + start + "&pageSize=" + pageSize, method, operationMetadata.Credentials, convention)).AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
            {
                RavenJToken result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);

                List<Attachment> attachments = convention.CreateSerializer().Deserialize<Attachment[]>(new RavenJTokenReader(result)).Select(x => new Attachment
                {
                    Etag = x.Etag,
                    Metadata = x.Metadata.WithCaseInsensitivePropertyNames(),
                    Size = x.Size,
                    Key = x.Key,
                    Data = () =>
                        { throw new InvalidOperationException("Cannot get attachment data from an attachment header"); }
                }).ToList();

                return new AsyncEnumeratorBridge<Attachment>(attachments.GetEnumerator());
            }
        }

#if !DNXCORE50
        public Task CommitAsync(string txId, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("POST", operationMetadata => DirectCommit(txId, operationMetadata, token), token);
        }

        private async Task DirectCommit(string txId, OperationMetadata operationMetadata, CancellationToken token)
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/transaction/commit?tx=" + txId, "POST", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)).AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
            {
                await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
            }
        }

        public Task RollbackAsync(string txId, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("POST", operationMetadata => DirectRollback(txId, operationMetadata, token), token);
        }

        private async Task DirectRollback(string txId, OperationMetadata operationMetadata, CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/transaction/rollback?tx=" + txId, "POST", operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)).AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
            {
                await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
            }
        }

        public Task PrepareTransactionAsync(string txId, Guid? resourceManagerId = null, byte[] recoveryInformation = null, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication("POST", operationMetadata => DirectPrepareTransaction(txId, operationMetadata, resourceManagerId, recoveryInformation, token), token);
        }

        private async Task DirectPrepareTransaction(string txId, OperationMetadata operationMetadata, Guid? resourceManagerId, byte[] recoveryInformation, CancellationToken token = default(CancellationToken))
        {
            var opUrl = operationMetadata.Url + "/transaction/prepare?tx=" + txId;
            if (resourceManagerId != null)
                opUrl += "&resourceManagerId=" + resourceManagerId;

            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, opUrl, "POST", operationMetadata.Credentials, convention)
                .AddOperationHeaders(OperationsHeaders))
                .AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
            {
                if (recoveryInformation != null)
                {
                    var ms = new MemoryStream(recoveryInformation);
                    await request.WriteAsync(ms).WithCancellation(token).ConfigureAwait(false);
                }

                await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
            }
        }
#endif

        private void HandleReplicationStatusChanges(NameValueCollection headers, string primaryUrl, string currentUrl)
        {
            if (primaryUrl.Equals(currentUrl, StringComparison.OrdinalIgnoreCase))
                return;

            var forceCheck = headers[Constants.RavenForcePrimaryServerCheck];
            bool shouldForceCheck;
            if (!string.IsNullOrEmpty(forceCheck) && bool.TryParse(forceCheck, out shouldForceCheck))
            {
                replicationInformer.ForceCheck(primaryUrl, shouldForceCheck);
            }
        }

        internal Task ExecuteWithReplication(string method, Func<OperationMetadata, Task> operation, CancellationToken token = default(CancellationToken))
        {
            // Convert the Func<string, Task> to a Func<string, Task<object>>
            return ExecuteWithReplication(method, u => operation(u).ContinueWith<object>(t =>
            {
                t.AssertNotFailed();
                return null;
            }, token), token);
        }

        private volatile bool currentlyExecuting;
        private volatile bool retryBecauseOfConflict;
        private bool resolvingConflict;
        private bool resolvingConflictRetries;

        internal async Task<T> ExecuteWithReplication<T>(string method, Func<OperationMetadata, Task<T>> operation, CancellationToken token = default(CancellationToken))
        {
            var currentRequest = Interlocked.Increment(ref requestCount);
            if (currentlyExecuting && convention.AllowMultipuleAsyncOperations == false && retryBecauseOfConflict == false)
                throw new InvalidOperationException("Only a single concurrent async request is allowed per async client instance.");
            currentlyExecuting = true;
            try
            {
                return await replicationInformer
                    .ExecuteWithReplicationAsync(method, Url, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, currentRequest, readStripingBase, operation, token).ConfigureAwait(false);
            }
            finally
            {
                currentlyExecuting = false;
            }
        }

        private async Task<bool> AssertNonConflictedDocumentAndCheckIfNeedToReload(OperationMetadata operationMetadata, RavenJObject docResult,
                                                                                    Func<string, ConflictException> onConflictedQueryResult = null, CancellationToken token = default(CancellationToken))
        {
            if (docResult == null)
                return (false);
            var metadata = docResult[Constants.Metadata];
            if (metadata == null)
                return (false);

            if (metadata.Value<int>("@Http-Status-Code") == 409)
            {
                var etag = HttpExtensions.EtagHeaderToEtag(metadata.Value<string>("@etag"));
                var e = await TryResolveConflictOrCreateConcurrencyException(operationMetadata, metadata.Value<string>("@id"), docResult, etag, token).ConfigureAwait(false);
                if (e != null)
                    throw e;
                return true;

            }

            if (metadata.Value<bool>(Constants.RavenReplicationConflict) && onConflictedQueryResult != null)
                throw onConflictedQueryResult(metadata.Value<string>("@id"));

            return (false);
        }

        private async Task<ConflictException> TryResolveConflictOrCreateConcurrencyException(OperationMetadata operationMetadata, string key,
                                                                                             RavenJObject conflictsDoc,
                                                                                             Etag etag,
                                                                                             CancellationToken token)
        {
            var ravenJArray = conflictsDoc.Value<RavenJArray>("Conflicts");
            if (ravenJArray == null)
                throw new InvalidOperationException(
                    "Could not get conflict ids from conflicted document, are you trying to resolve a conflict when using metadata-only?");

            var conflictIds = ravenJArray.Select(x => x.Value<string>()).ToArray();

            var result = await TryResolveConflictByUsingRegisteredListenersAsync(key, etag, conflictIds, operationMetadata, token).ConfigureAwait(false);
            if (result)
                return null;

            return
                new ConflictException(
                    "Conflict detected on " + key + ", conflict must be resolved before the document will be accessible",
                                                 true)
                {
                    ConflictedVersionIds = conflictIds,
                    Etag = etag
                };
        }

        internal async Task<bool> TryResolveConflictByUsingRegisteredListenersAsync(string key, Etag etag, string[] conflictIds, OperationMetadata operationMetadata = null, CancellationToken token = default(CancellationToken))
        {
            if (operationMetadata == null)
                operationMetadata = new OperationMetadata(Url);

            if (conflictListeners.Length > 0 && resolvingConflict == false)
            {
                resolvingConflict = true;
                try
                {
                    var result = await DirectGetAsync(operationMetadata, conflictIds, null, null, null, false, token).ConfigureAwait(false);
                    var results = result.Results.Select(SerializationHelper.ToJsonDocument).ToArray();

                    foreach (var conflictListener in conflictListeners)
                    {
                        JsonDocument resolvedDocument;
                        if (conflictListener.TryResolveConflict(key, results, out resolvedDocument))
                        {
                            await DirectPutAsync(operationMetadata, key, etag, resolvedDocument.DataAsJson, resolvedDocument.Metadata, token).ConfigureAwait(false);
                            return true;
                        }
                    }

                    return false;
                }
                finally
                {
                    resolvingConflict = false;
                }
            }

            return false;
        }

        private async Task<T> RetryOperationBecauseOfConflict<T>(OperationMetadata operationMetadata, IEnumerable<RavenJObject> docResults,
                                                                 T currentResult, Func<Task<T>> nextTry, Func<string, ConflictException> onConflictedQueryResult = null, CancellationToken token = default(CancellationToken))
        {
            bool requiresRetry = false;
            foreach (var docResult in docResults)
            {
                token.ThrowIfCancellationRequested();
                requiresRetry |=
                    await AssertNonConflictedDocumentAndCheckIfNeedToReload(operationMetadata, docResult, onConflictedQueryResult, token).ConfigureAwait(false);
            }

            if (!requiresRetry)
                return currentResult;

            if (resolvingConflictRetries)
                throw new InvalidOperationException(
                    "Encountered another conflict after already resolving a conflict. Conflict resolution cannot recurse.");
            resolvingConflictRetries = true;
            retryBecauseOfConflict = true;
            try
            {
                return await nextTry().WithCancellation(token).ConfigureAwait(false);
            }
            finally
            {
                resolvingConflictRetries = false;
                retryBecauseOfConflict = false;
            }
        }

        public async Task<RavenJToken> GetOperationStatusAsync(long id)
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url + "/operation/status?id=" + id, "GET", credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention).AddOperationHeaders(OperationsHeaders)))
            {
                try
                {
                    return await request.ReadResponseJsonAsync().ConfigureAwait(false);
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode == HttpStatusCode.NotFound)
                        return null;

                    throw;
                }
            }
        }

        public IAsyncInfoDatabaseCommands Info
        {
            get { return this; }
        }

        async Task<ReplicationStatistics> IAsyncInfoDatabaseCommands.GetReplicationInfoAsync(CancellationToken token)
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url.ReplicationInfo(), "GET", credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention)))
            {
                var json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return json.Deserialize<ReplicationStatistics>(convention);
            }
        }

        public IAsyncDatabaseCommands With(ICredentials credentialsForSession)
        {
            return WithInternal(credentialsForSession);
        }

        internal AsyncServerClient WithInternal(ICredentials credentialsForSession)
        {
            return new AsyncServerClient(url, convention, new OperationCredentials(credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication.ApiKey, credentialsForSession), jsonRequestFactory, sessionId,
                                         replicationInformerGetter, databaseName, conflictListeners, false);
        }

        internal async Task<ReplicationDocument> DirectGetReplicationDestinationsAsync(OperationMetadata operationMetadata)
        {
            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/replication/topology", "GET", operationMetadata.Credentials, convention);
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams.AddOperationHeaders(OperationsHeaders)).AddReplicationStatusHeaders(url, operationMetadata.Url, replicationInformer, convention.FailoverBehavior, HandleReplicationStatusChanges))
            {
                try
                {
                    var requestJson = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    return requestJson.JsonDeserialization<ReplicationDocument>();
                }
                catch (ErrorResponseException e)
                {
                    switch (e.StatusCode)
                    {
                        case HttpStatusCode.NotFound:
                        case HttpStatusCode.BadRequest: //replication bundle if not enabled
                            return null;
                        default:
                            throw;
                    }
                }
            }
        }
    }
}
