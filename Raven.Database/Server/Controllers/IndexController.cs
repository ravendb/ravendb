using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Database.Actions;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Queries;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Storage;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

namespace Raven.Database.Server.Controllers
{
    public class IndexController : ClusterAwareRavenDbApiController
    {
        [HttpGet]
        [RavenRoute("indexes")]
        [RavenRoute("databases/{databaseName}/indexes")]
        public HttpResponseMessage IndexesGet()
        {
            var namesOnlyString = GetQueryStringValue("namesOnly");
            bool namesOnly;
            RavenJArray indexes;
            if (bool.TryParse(namesOnlyString, out namesOnly) && namesOnly)
                indexes = Database.Indexes.GetIndexNames(GetStart(), GetPageSize(Database.Configuration.MaxPageSize));
            else
                indexes = Database.Indexes.GetIndexes(GetStart(), GetPageSize(Database.Configuration.MaxPageSize));

            return GetMessageWithObject(indexes);
        }

        [HttpPut]
        [RavenRoute("indexes")]
        [RavenRoute("databases/{databaseName}/indexes")]
        public async Task<HttpResponseMessage> IndexMultiPut()
        {
            IndexToAdd[] indexesToAdd;
            try
            {
                indexesToAdd = await ReadJsonObjectAsync<IndexToAdd[]>().ConfigureAwait(false);
            }
            catch (InvalidOperationException e)
            {
                if (Log.IsDebugEnabled)
                    Log.DebugException("Failed to deserialize index request. Error: ", e);
                return GetMessageWithObject(new
                {
                    Message = "Could not understand json, please check its validity.",
                    Error = e.Message
                }, (HttpStatusCode)500);

            }
            catch (InvalidDataException e)
            {
                if (Log.IsDebugEnabled)
                    Log.DebugException("Failed to deserialize index request. Error: ", e);
                
                return GetMessageWithObject(new
                {
                    Error = e
                }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
            }

            foreach (var indexToAdd in indexesToAdd)
            {
                var data = indexToAdd.Definition;
                if (data == null || (data.Map == null && (data.Maps == null || data.Maps.Count == 0)))
                    return GetMessageWithString("Expected json document with 'Map' or 'Maps' property", HttpStatusCode.BadRequest);
            }

            string[] createdIndexes;
            try
            {
                createdIndexes = Database.Indexes.PutIndexes(indexesToAdd);
            }
            catch (Exception ex)
            {
                var compilationException = ex as IndexCompilationException;

                return GetMessageWithObject(new
                {
                    ex.Message,
                    IndexDefinitionProperty = compilationException != null ? compilationException.IndexDefinitionProperty : "",
                    ProblematicText = compilationException != null ? compilationException.ProblematicText : "",
                    Error = ex.ToString()
                }, HttpStatusCode.BadRequest);
            }
            return GetMessageWithObject(new { Indexes = createdIndexes }, HttpStatusCode.Created);
        }

        [HttpPut]
        [RavenRoute("side-by-side-indexes")]
        [RavenRoute("databases/{databaseName}/side-by-side-indexes")]
        public async Task<HttpResponseMessage> SideBySideIndexMultiPut()
        {
            SideBySideIndexes sideBySideIndexes;
            try
            {
                sideBySideIndexes = await ReadJsonObjectAsync<SideBySideIndexes>().ConfigureAwait(false);
            }
            catch (InvalidOperationException e)
            {
                if (Log.IsDebugEnabled)
                    Log.DebugException("Failed to deserialize index request. Error: ", e);
                return GetMessageWithObject(new
                {
                    Message = "Could not understand json, please check its validity.",
                    Error = e.Message
                }, (HttpStatusCode)500);

            }
            catch (InvalidDataException e)
            {
                if (Log.IsDebugEnabled)
                    Log.DebugException("Failed to deserialize index request. Error: ", e);

                return GetMessageWithObject(new
                {
                    Error = e
                }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
            }

            foreach (var indexToAdd in sideBySideIndexes.IndexesToAdd)
            {
                var data = indexToAdd.Definition;
                if (data == null || (data.Map == null && (data.Maps == null || data.Maps.Count == 0)))
                    return GetMessageWithString("Expected json document with 'Map' or 'Maps' property", HttpStatusCode.BadRequest);
            }

            List<IndexActions.IndexInfo> createdIndexes;
            try
            {
                createdIndexes = Database.Indexes.PutSideBySideIndexes(sideBySideIndexes);
            }
            catch (Exception ex)
            {
                var compilationException = ex as IndexCompilationException;

                return GetMessageWithObject(new
                {
                    ex.Message,
                    IndexDefinitionProperty = compilationException != null ? compilationException.IndexDefinitionProperty : "",
                    ProblematicText = compilationException != null ? compilationException.ProblematicText : "",
                    Error = ex.ToString()
                }, HttpStatusCode.BadRequest);
            }

            return GetMessageWithObject(new { Indexes = createdIndexes.Select(x => x.Name).ToArray() }, HttpStatusCode.Created);
        }

        [HttpGet]
        [RavenRoute("indexes/{*id}")]
        [RavenRoute("databases/{databaseName}/indexes/{*id}")]
        public HttpResponseMessage IndexGet(string id)
        {
            using (var cts = new CancellationTokenSource())
            using (cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
            {
                var index = id;
                if (string.IsNullOrEmpty(GetQueryStringValue("definition")) == false) 
                    return GetIndexDefinition(index);

                if (string.IsNullOrEmpty(GetQueryStringValue("source")) == false) 
                    return GetIndexSource(index);

                if (string.IsNullOrEmpty(GetQueryStringValue("debug")) == false) 
                    return DebugIndex(index);

                if (string.IsNullOrEmpty(GetQueryStringValue("explain")) == false) 
                    return GetExplanation(index);

                try
                {
                return GetIndexQueryResult(index, cts.Token);
            }
                catch (OperationCanceledException e)
                {
                    throw new TimeoutException(string.Format("The query did not produce results in {0}", DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout), e);
        }
            }
        }

        [HttpPost]
        [RavenRoute("indexes/last-queried")]
        [RavenRoute("databases/{databaseName}/indexes/last-queried")]
        public HttpResponseMessage IndexUpdateLastQueried([FromBody] Dictionary<string, DateTime> lastQueriedByIndexId)
        {
            Database.TransactionalStorage.Batch(accessor =>
            {
                foreach (var timestamp in lastQueriedByIndexId)
                {
                    var indexInstance = Database.IndexStorage.GetIndexInstance(timestamp.Key);
                    if (indexInstance == null) continue;
                    indexInstance.MarkQueried(timestamp.Value);
                }
            });
            return GetEmptyMessage();
        }

        [HttpPut]
        [RavenRoute("indexes/{*id}")]
        [RavenRoute("databases/{databaseName}/indexes/{*id}")]
        public async Task<HttpResponseMessage> IndexPut(string id)
        {
            var index = id;
            RavenJObject jsonIndex;

            try
            {
                jsonIndex = await ReadJsonAsync().ConfigureAwait(false);
            }
            catch (InvalidOperationException e)
            {
                if (Log.IsDebugEnabled)
                    Log.DebugException("Failed to deserialize index request. Error: ", e);
                return GetMessageWithObject(new
                {
                    Message = "Could not understand json, please check its validity.",
                    Error = e.Message
                }, (HttpStatusCode)500); 

            }
            catch (InvalidDataException e)
            {
                if (Log.IsDebugEnabled)
                    Log.DebugException("Failed to deserialize index request. Error: ", e);
                return GetMessageWithObject(new
                {
                    e.Message
                }, (HttpStatusCode)422); //http code 422 - Unprocessable entity
            }

            var data = jsonIndex.JsonDeserialization<IndexDefinition>();

            if (data == null || (data.Map == null && (data.Maps == null || data.Maps.Count == 0)))
                return GetMessageWithString("Expected json document with 'Map' or 'Maps' property", HttpStatusCode.BadRequest);

            // older clients (pre 3.0) might try to create the index without MaxIndexOutputsPerDocument set
            // in order to ensure that they don't reset the default value for old clients, we force the default
            // value to maintain the existing behavior
            if (jsonIndex.ContainsKey("MaxIndexOutputsPerDocument") == false)
                data.MaxIndexOutputsPerDocument = 16 * 1024;

            try
            {
                long opId;
                Database.Indexes.PutIndex(index, data, out opId);

                //treat includePrecomputeOperation as a flag
                var includePrecomputeOperation = GetQueryStringValue("includePrecomputeOperation");
                if (!String.IsNullOrWhiteSpace(includePrecomputeOperation) &&
                    includePrecomputeOperation.Equals("yes",StringComparison.OrdinalIgnoreCase))
                {
                    return GetMessageWithObject(new { Index = index, OperationId = opId }, HttpStatusCode.Created);
                }

                return GetMessageWithObject(new { Index = index }, HttpStatusCode.Created);
            }
            catch (Exception ex)
            {
                var compilationException = ex as IndexCompilationException;

                Log.ErrorException("Cannot create index.", ex);

                return GetMessageWithObject(new
                {
                    ex.Message,
                    IndexDefinitionProperty = compilationException != null ? compilationException.IndexDefinitionProperty : "",
                    ProblematicText = compilationException != null ? compilationException.ProblematicText : "",
                    Error = ex.ToString()
                }, HttpStatusCode.BadRequest);
            }
        }

        [HttpHead]
        [RavenRoute("indexes/{*id}")]
        [RavenRoute("databases/{databaseName}/indexes/{*id}")]
        public HttpResponseMessage IndexHead(string id)
        {
            var index = id;
            if (Database.IndexDefinitionStorage.IndexNames.Contains(index, StringComparer.OrdinalIgnoreCase) == false)
                return GetEmptyMessage(HttpStatusCode.NotFound);
            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("indexes/{*id}")]
        [RavenRoute("databases/{databaseName}/indexes/{*id}")]
        public async Task<HttpResponseMessage> IndexPost(string id)
        {
            var index = id;
            /*
           This is a workaround to support version 30037 and below where index post conflicts with index set priorety
           */
            if (id.StartsWith("set-priority/"))
            {
                SetPriorityInternal(id.Substring("set-priority/".Length));
            }

            if ("forceReplace".Equals(GetQueryStringValue("op"), StringComparison.InvariantCultureIgnoreCase))
            {
                var indexDefiniton = Database.IndexDefinitionStorage.GetIndexDefinition(id);
                if (indexDefiniton == null)
                    return GetEmptyMessage(HttpStatusCode.NotFound);

                Database.IndexReplacer.ForceReplacement(indexDefiniton);
                return GetEmptyMessage();
            }

            if ("forceWriteToDisk".Equals(GetQueryStringValue("op"), StringComparison.InvariantCultureIgnoreCase))
            {
                Database.IndexStorage.ForceWriteToDiskAndWriteInMemoryIndexToDiskIfNecessary(index);
                return GetEmptyMessage();
            }

            if ("hasChanged".Equals(GetQueryStringValue("op"), StringComparison.InvariantCultureIgnoreCase))
            {
                var data = await ReadJsonObjectAsync<IndexDefinition>().ConfigureAwait(false);
                if (data == null || (data.Map == null && (data.Maps == null || data.Maps.Count == 0)))
                    return GetMessageWithString("Expected json document with 'Map' or 'Maps' property", HttpStatusCode.BadRequest);

                return GetMessageWithObject(new { Name = index, Changed = Database.Indexes.IndexHasChanged(index, data) });
            }

            if ("lockModeChange".Equals(GetQueryStringValue("op"), StringComparison.InvariantCultureIgnoreCase))
                return HandleIndexLockModeChange(index);

            if ("true".Equals(GetQueryStringValue("postQuery"), StringComparison.InvariantCultureIgnoreCase))
            {
                var postedQuery = await ReadStringAsync().ConfigureAwait(false);
                
                SetPostRequestQuery(postedQuery);
                return IndexGet(id);
            }

            return GetMessageWithString("Not idea how to handle a POST on " + index + " with op=" +
                                        (GetQueryStringValue("op") ?? "<no val specified>"));
        }

        [HttpReset]
        [RavenRoute("indexes/{*id}")]
        [RavenRoute("databases/{databaseName}/indexes/{*id}")]
        public HttpResponseMessage IndexReset(string id)
        {
            var index = id;
            Database.Indexes.ResetIndex(index);
            return GetMessageWithObject(new { Reset = index });
        }

        [HttpDelete]
        [RavenRoute("indexes/{*id}")]
        [RavenRoute("databases/{databaseName}/indexes/{*id}")]
        public HttpResponseMessage IndexDelete(string id)
        {
            var index = id;

            var isReplication = GetQueryStringValue(Constants.IsReplicatedUrlParamName);
            if (Database.Indexes.DeleteIndex(index) &&
                !string.IsNullOrWhiteSpace(isReplication) && isReplication.Equals("true", StringComparison.InvariantCultureIgnoreCase))
            {
                const string emptyFrom = "<no hostname>";
                var from = Uri.UnescapeDataString(GetQueryStringValue("from") ?? emptyFrom);
                Log.Info("received index deletion from replication (replicating index tombstone, received from = {0})", from);
            }

            return GetEmptyMessage(HttpStatusCode.NoContent);
        }

        [HttpPost]
        [RavenRoute("indexes-rename/{*id}")]
        [RavenRoute("databases/{databaseName}/indexes-rename/{*id}")]
        public HttpResponseMessage Rename(string id)
        {
            var newIndexName = GetQueryStringValue("newName");

            var instance = Database.Indexes.GetIndexDefinition(id);
            if (instance == null)
                throw new IndexDoesNotExistsException(string.Format("Index '{0}' does not exist.", id));

            if (Database.Indexes.GetIndexDefinition(newIndexName) != null)
                throw new InvalidOperationException($"Cannot rename to {newIndexName}. Index already exists.");

            Database.Indexes.RenameIndex(instance, newIndexName);

            return GetEmptyMessage();
        }

        [HttpPost]
        [RavenRoute("indexes/set-priority/{*id}")]
        [RavenRoute("databases/{databaseName}/indexes/set-priority/{*id}")]
        public HttpResponseMessage SetPriority(string id)
        {
            return SetPriorityInternal(id);
        }

        [HttpPost]
        [RavenRoute("indexes-set-priority/{*id}")]
        [RavenRoute("databases/{databaseName}/indexes-set-priority/{*id}")]
        public HttpResponseMessage SetPriorityConflicFixed(string id)
        {
            return SetPriorityInternal(id);
        }

        private HttpResponseMessage SetPriorityInternal(string id)
        {
            var index = id;

            IndexingPriority indexingPriority;
            if (Enum.TryParse(GetQueryStringValue("priority"), out indexingPriority) == false)
            {
                return GetMessageWithObject(new
                {
                    Error = "Could not parse priority value: " + GetQueryStringValue("priority")
                }, HttpStatusCode.BadRequest);
            }

            var instance = Database.IndexStorage.GetIndexInstance(index);
            var oldPriority = instance.Priority;
            Database.TransactionalStorage.Batch(accessor => accessor.Indexing.SetIndexPriority(instance.indexId, indexingPriority));
            instance.Priority = indexingPriority;

            if (oldPriority == IndexingPriority.Disabled &&
                (indexingPriority == IndexingPriority.Normal || indexingPriority == IndexingPriority.Idle))
            {
                Database.WorkContext.NotifyAboutWork();
            }

            return GetEmptyMessage();
        }

        [HttpPatch]
        [RavenRoute("indexes/try-recover-corrupted")]
        [RavenRoute("databases/{databaseName}/indexes/try-recover-corrupted")]
        public HttpResponseMessage TryRecoverCorruptedIndexes()
        {
            foreach (var indexId in Database.IndexStorage.Indexes)
            {
                var index = Database.IndexStorage.GetIndexInstance(indexId);

                if(index.Priority != IndexingPriority.Error)
                    continue;

                long taskId;
                var task = Task.Run(() =>
                {
                    // try to recover by reopening the index - it will reset it if necessary
                    try
                    {
                        index = Database.IndexStorage.ReopenCorruptedIndex(index);

                        Database.TransactionalStorage.Batch(accessor => accessor.Indexing.SetIndexPriority(index.IndexId, IndexingPriority.Normal));
                        index.Priority = IndexingPriority.Normal;

                        Database.WorkContext.ShouldNotifyAboutWork(() => string.Format("Index {0} has been recovered.", index.PublicName));
                        Database.WorkContext.NotifyAboutWork();
                    }
                    catch (Exception e)
                    {
                        Log.WarnException("Failed to recover the corrupted index '{0}' by reopening it.", e);
                    }
                });

                Database.Tasks.AddTask(task, new TaskBasedOperationState(task), new TaskActions.PendingTaskDescription
                {
                    StartTime = SystemTime.UtcNow,
                    TaskType = TaskActions.PendingTaskType.RecoverCorruptedIndexOperation,
                    Description = index.PublicName
                }, out taskId);
            }

            return GetEmptyMessage(HttpStatusCode.Accepted);
        }

        [HttpGet]
        [RavenRoute("c-sharp-index-definition/{*fullIndexName}")]
        [RavenRoute("databases/{databaseName}/c-sharp-index-definition/{*fullIndexName}")]
        public HttpResponseMessage GenerateCSharpIndexDefinition(string fullIndexName)
        {
            var indexDefinition = Database.Indexes.GetIndexDefinition(fullIndexName);
            if (indexDefinition == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            var text = new IndexDefinitionCodeGenerator(indexDefinition).Generate();

            return GetMessageWithObject(text);
        }

        private HttpResponseMessage GetIndexDefinition(string index)
        {
            var indexDefinition = Database.Indexes.GetIndexDefinition(index);
            if (indexDefinition == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            indexDefinition.Fields = Database.Indexes.GetIndexFields(index);

            return GetMessageWithObject(new
            {
                Index = indexDefinition,
            });
        }

        private HttpResponseMessage GetIndexSource(string index)
        {
            var viewGenerator = Database.IndexDefinitionStorage.GetViewGenerator(index);
            if (viewGenerator == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            return GetMessageWithString(viewGenerator.SourceCode);
        }

        private HttpResponseMessage DebugIndex(string index)
        {
            switch (GetQueryStringValue("debug").ToLowerInvariant())
            {
                case "docs":
                    return GetDocsStartsWith(index);
                case "map":
                    return GetIndexMappedResult(index);
                case "reduce":
                    return GetIndexReducedResult(index);
                case "schedules":
                    return GetIndexScheduledReduces(index);
                case "keys":
                    return GetIndexKeysStats(index);
                case "entries":
                    return GetIndexEntries(index);
                case "stats":
                    return GetIndexStats(index);
                default:
                    return GetMessageWithString("Unknown debug option " + GetQueryStringValue("debug"), HttpStatusCode.BadRequest);
            }
        }

        private HttpResponseMessage GetDocsStartsWith(string index)
        {
            var definition = Database.IndexDefinitionStorage.GetIndexDefinition(index);
            if (definition == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            var prefix = GetQueryStringValue("startsWith");
            List<string> keys = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                keys = accessor.MapReduce.GetSourcesForIndexForDebug(definition.IndexId, prefix, GetPageSize(Database.Configuration.MaxPageSize))
                    .ToList(); 
            });
            return GetMessageWithObject(new {keys.Count, Results = keys});
        }

        private HttpResponseMessage GetIndexMappedResult(string index)
        {
            var definition = Database.IndexDefinitionStorage.GetIndexDefinition(index);
            if (definition == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            var key = GetQueryStringValue("key");
            if (string.IsNullOrEmpty(key))
            {
                var sourceId = GetQueryStringValue("sourceId");
                var startsWith = GetQueryStringValue("startsWith");

                List<string> keys = null;
                Database.TransactionalStorage.Batch(accessor =>
                {
                    keys = accessor.MapReduce.GetKeysForIndexForDebug(definition.IndexId, startsWith, sourceId, GetStart(), GetPageSize(Database.Configuration.MaxPageSize))
                        .ToList();
                });

                return GetMessageWithObject(new
                {
                    keys.Count,
                    Results = keys
                });
            }

            List<MappedResultInfo> mappedResult = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                mappedResult = accessor.MapReduce.GetMappedResultsForDebug(definition.IndexId, key, GetStart(), GetPageSize(Database.Configuration.MaxPageSize))
                    .ToList();
            });
            return GetMessageWithObject(new
            {
                mappedResult.Count,
                Results = mappedResult
            });
        }

        private HttpResponseMessage GetExplanation(string index)
        {
            var dynamicIndex = index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) ||
                               index.Equals("dynamic", StringComparison.OrdinalIgnoreCase);

            if (dynamicIndex == false)
            {
                return GetMessageWithObject(new
                {
                    Error = "Explain can only work on dynamic indexes"
                }, HttpStatusCode.BadRequest);
            }

            var indexQuery = GetIndexQuery(Database.Configuration.MaxPageSize);
            string entityName = null;
            if (index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase))
                entityName = index.Substring("dynamic/".Length);

            var explanations = Database.ExplainDynamicIndexSelection(entityName, indexQuery);

            return GetMessageWithObject(explanations);
        }

        private HttpResponseMessage GetIndexQueryResult(string index, CancellationToken token)
        {
            Etag indexEtag;
            var msg = GetEmptyMessage();
            var queryResult = ExecuteQuery(index, out indexEtag, msg, token);

            if (queryResult == null)
                return msg;

            var includes = GetQueryStringValues("include") ?? new string[0];
            var loadedIds = new HashSet<string>(
                queryResult.Results
                    .Where(x => x != null && x["@metadata"] != null)
                    .Select(x => x["@metadata"].Value<string>("@id"))
                    .Where(x => x != null)
                );
            var command = new AddIncludesCommand(Database, GetRequestTransaction(),
                                                 (etag, doc) => queryResult.Includes.Add(doc), includes, loadedIds);
            foreach (var result in queryResult.Results)
            {
                command.Execute(result);
            }
            command.AlsoInclude(queryResult.IdsToInclude);

            return GetMessageWithObject(queryResult, queryResult.NonAuthoritativeInformation ? HttpStatusCode.NonAuthoritativeInformation : HttpStatusCode.OK, indexEtag);
        }

        private QueryResultWithIncludes ExecuteQuery(string index, out Etag indexEtag, HttpResponseMessage msg, CancellationToken token)
        {
            var indexQuery = GetIndexQuery(Database.Configuration.MaxPageSize);
            RewriteDateQueriesFromOldClients(indexQuery);

            var sp = Stopwatch.StartNew();
            var result = index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) || index.Equals("dynamic", StringComparison.OrdinalIgnoreCase) ?
                PerformQueryAgainstDynamicIndex(index, indexQuery, out indexEtag, msg, token) :
                PerformQueryAgainstExistingIndex(index, indexQuery, out indexEtag, msg, token);

            sp.Stop();
            if (Log.IsDebugEnabled)
                Log.Debug(() =>
                {
                    var sb = new StringBuilder();
                    ReportQuery(sb, indexQuery, sp, result);
                    return sb.ToString();
                });
            AddRequestTraceInfo(sb => ReportQuery(sb, indexQuery, sp, result));

            return result;
        }

        private static void ReportQuery(StringBuilder sb, IndexQuery indexQuery, Stopwatch sp, QueryResultWithIncludes result)
        {
            sb.Append("\tQuery: ")
                .Append(indexQuery.Query)
                .AppendLine();
            sb.Append("\t").AppendFormat("Time: {0:#,#;;0} ms", sp.ElapsedMilliseconds).AppendLine();

            if (result == null)
                return;

            sb.Append("\tIndex: ")
                .AppendLine(result.IndexName);
            sb.Append("\t").AppendFormat("Results: {0:#,#;;0} returned out of {1:#,#;;0} total.", result.Results.Count, result.TotalResults).AppendLine();

            if (result.TimingsInMilliseconds != null)
            {
                sb.Append("\tTiming:").AppendLine();
                foreach (var timing in result.TimingsInMilliseconds)
                {
                    sb.Append("\t").Append(timing.Key).Append(": ").Append(timing.Value).AppendLine();
                }
            }
        }

        private QueryResultWithIncludes PerformQueryAgainstExistingIndex(string index, IndexQuery indexQuery, out Etag indexEtag, HttpResponseMessage msg, CancellationToken token)
        {
            indexEtag = Database.Indexes.GetIndexEtag(index, null, indexQuery.ResultsTransformer);

            if (MatchEtag(indexEtag))
            {
                Database.IndexStorage.MarkCachedQuery(index);
                msg.StatusCode = HttpStatusCode.NotModified;
                return null;
            }

            var queryResult = Database.Queries.Query(index, indexQuery, token);
            indexEtag = Database.Indexes.GetIndexEtag(index, queryResult.ResultEtag, indexQuery.ResultsTransformer);

            Database.IndexStorage.SetLastQueryTime(queryResult.IndexName, queryResult.LastQueryTime);
            return queryResult;
        }

        private QueryResultWithIncludes PerformQueryAgainstDynamicIndex(string index, IndexQuery indexQuery, out Etag indexEtag, HttpResponseMessage msg, CancellationToken token)
        {
            string entityName;
            var dynamicIndexName = GetDynamicIndexName(index, indexQuery, out entityName);

            if (dynamicIndexName != null && Database.IndexStorage.HasIndex(dynamicIndexName))
            {
                indexEtag = Database.Indexes.GetIndexEtag(dynamicIndexName, null, indexQuery.ResultsTransformer);
                if (MatchEtag(indexEtag))
                {
                    Database.IndexStorage.MarkCachedQuery(dynamicIndexName);
                    msg.StatusCode = HttpStatusCode.NotModified;
                    return null;
                }
            }

            if (dynamicIndexName == null && // would have to create a dynamic index
                Database.Configuration.CreateAutoIndexesForAdHocQueriesIfNeeded == false) // but it is disabled
            {
                indexEtag = Etag.InvalidEtag;
                var explanations = Database.ExplainDynamicIndexSelection(entityName, indexQuery);

                msg.StatusCode = HttpStatusCode.BadRequest;
                
                var target = entityName == null ? "all documents" : entityName + " documents";

                msg.Content = JsonContent(RavenJToken.FromObject(
                    new
                    {
                        Error =
                            "Executing the query " + indexQuery.Query + " on " + target +
                            " require creation of temporary index, and it has been explicitly disabled.",
                        Explanations = explanations
                    }));
                return null;
            }

            var queryResult = Database.ExecuteDynamicQuery(entityName, indexQuery, token);

            // have to check here because we might be getting the index etag just 
            // as we make a switch from temp to auto, and we need to refresh the etag
            // if that is the case. This can also happen when the optimizer
            // decided to switch indexes for a query.
            indexEtag = (dynamicIndexName == null || queryResult.IndexName == dynamicIndexName)
                            ? Database.Indexes.GetIndexEtag(queryResult.IndexName, queryResult.ResultEtag, indexQuery.ResultsTransformer)
                            : Etag.InvalidEtag;

            Database.IndexStorage.SetLastQueryTime(queryResult.IndexName, queryResult.LastQueryTime);
            return queryResult;
        }

        private string GetDynamicIndexName(string index, IndexQuery indexQuery, out string entityName)
        {
            entityName = null;
            if (index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase))
                entityName = index.Substring("dynamic/".Length);

            var dynamicIndexName = Database.FindDynamicIndexName(entityName, indexQuery);
            return dynamicIndexName;
        }

        static Regex oldDateTimeFormat = new Regex(@"(\:|\[|{|TO\s) \s* (\d{17})", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

        private void RewriteDateQueriesFromOldClients(IndexQuery indexQuery)
        {
            var clientVersion = GetHeader("Raven-Client-Version");
            if (string.IsNullOrEmpty(clientVersion) == false) // new client
                return;

            var matches = oldDateTimeFormat.Matches(indexQuery.Query);
            if (matches.Count == 0)
                return;
            var builder = new StringBuilder(indexQuery.Query);
            for (int i = matches.Count - 1; i >= 0; i--) // working in reverse so as to avoid invalidating previous indexes
            {
                var dateTimeString = matches[i].Groups[2].Value;

                DateTime time;
                if (DateTime.TryParseExact(dateTimeString, "yyyyMMddHHmmssfff", CultureInfo.InvariantCulture, DateTimeStyles.None, out time) == false)
                    continue;

                builder.Remove(matches[i].Groups[2].Index, matches[i].Groups[2].Length);
                var newDateTimeFormat = time.ToString(Default.DateTimeFormatsToWrite);
                builder.Insert(matches[i].Groups[2].Index, newDateTimeFormat);
            }
            indexQuery.Query = builder.ToString();
        }

        private HttpResponseMessage HandleIndexLockModeChange(string index)
        {
            var lockModeStr = GetQueryStringValue("mode");

            IndexLockMode indexLockMode;
            if (Enum.TryParse(lockModeStr, out indexLockMode) == false)
                return GetMessageWithString("Cannot understand index lock mode: " + lockModeStr, HttpStatusCode.BadRequest);

            var indexDefinition = Database.IndexDefinitionStorage.GetIndexDefinition(index);
            if (indexDefinition == null)
                return GetMessageWithString("Cannot find index : " + index, HttpStatusCode.NotFound);
            
            indexDefinition.LockMode = indexLockMode;
            Database.IndexDefinitionStorage.UpdateIndexDefinitionWithoutUpdatingCompiledIndex(indexDefinition);

            return GetEmptyMessage();
        }

        private HttpResponseMessage GetIndexReducedResult(string index)
        {
            var definition = Database.IndexDefinitionStorage.GetIndexDefinition(index);
            if (definition == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);
            
            var key = GetQueryStringValue("key");
            if (string.IsNullOrEmpty(key))
                return GetMessageWithString("Query string argument 'key' is required", HttpStatusCode.BadRequest);

            int level;
            if (int.TryParse(GetQueryStringValue("level"), out level) == false || (level != 1 && level != 2))
                return GetMessageWithString("Query string argument 'level' is required and must be 1 or 2",
                    HttpStatusCode.BadRequest);

            List<MappedResultInfo> mappedResult = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                mappedResult = accessor.MapReduce.GetReducedResultsForDebug(definition.IndexId, key, level, GetStart(), GetPageSize(Database.Configuration.MaxPageSize))
                    .ToList();
            });

            return GetMessageWithObject(new
            {
                mappedResult.Count,
                Results = mappedResult
            });
        }

        private HttpResponseMessage GetIndexScheduledReduces(string index)
        {
            List<ScheduledReductionDebugInfo> mappedResult = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                var instance = Database.IndexStorage.GetIndexInstance(index);
                mappedResult = accessor.MapReduce.GetScheduledReductionForDebug(instance.indexId, GetStart(), GetPageSize(Database.Configuration.MaxPageSize))
                    .ToList();
            });

            return GetMessageWithObject(new
            {
                mappedResult.Count,
                Results = mappedResult
            });
        }

        private HttpResponseMessage GetIndexKeysStats(string index)
        {
            var definition = Database.IndexDefinitionStorage.GetIndexDefinition(index);
            if (definition == null)
            {
                return GetEmptyMessage(HttpStatusCode.NotFound);
            }

            List<ReduceKeyAndCount> keys = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                keys = accessor.MapReduce.GetKeysStats(definition.IndexId,
                         GetStart(),
                         GetPageSize(Database.Configuration.MaxPageSize))
                    .ToList();
            });

            return GetMessageWithObject(new
            {
                keys.Count,
                Results = keys
            });
        }

        private HttpResponseMessage GetIndexEntries(string index)
        {
            var indexQuery = GetIndexQuery(Database.Configuration.MaxPageSize);
            var reduceKeys = GetQueryStringValues("reduceKeys").Select(x => x.Trim()).ToList();

            if (string.IsNullOrEmpty(indexQuery.Query) == false && reduceKeys.Count > 0)
            {
                return GetMessageWithObject(new
                {
                    Error = "Cannot specity 'query' and 'reducedKeys' at the same time"
                }, HttpStatusCode.BadRequest);
            }

            if (reduceKeys.Count > 0)
            {
                // overwrite indexQueryPagining as __reduce_key field is not indexed, and we don't have simple method to obtain column alias
                indexQuery.Start = 0;
                indexQuery.PageSize = int.MaxValue;
            }
                

            var totalResults = new Reference<int>();

            var isDynamic = index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase)
                            || index.Equals("dynamic", StringComparison.OrdinalIgnoreCase);

            if (isDynamic)
                return GetIndexEntriesForDynamicIndex(index, indexQuery, reduceKeys, totalResults);

            return GetIndexEntriesForExistingIndex(index, indexQuery, reduceKeys, totalResults);
        }

        private HttpResponseMessage GetIndexEntriesForDynamicIndex(string index, IndexQuery indexQuery, List<string> reduceKeys, Reference<int> totalResults)
        {
            string entityName;
            var dynamicIndexName = GetDynamicIndexName(index, indexQuery, out entityName);

            if (dynamicIndexName == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            return GetIndexEntriesForExistingIndex(dynamicIndexName, indexQuery, reduceKeys, totalResults);
        }

        private HttpResponseMessage GetIndexEntriesForExistingIndex(string index, IndexQuery indexQuery, List<string> reduceKeys, Reference<int> totalResults)
        {
            var results = Database
                    .IndexStorage
                    .IndexEntires(index, indexQuery, reduceKeys, Database.IndexQueryTriggers, totalResults)
                    .ToArray();

            Tuple<DateTime, Etag> indexTimestamp = null;
            bool isIndexStale = false;

            var definition = Database.IndexDefinitionStorage.GetIndexDefinition(index);

            Database.TransactionalStorage.Batch(
                accessor =>
                {
                    isIndexStale = accessor.Staleness.IsIndexStale(definition.IndexId, indexQuery.Cutoff, indexQuery.CutoffEtag);
                    if (isIndexStale == false && indexQuery.Cutoff == null && indexQuery.CutoffEtag == null)
                    {
                        var indexInstance = Database.IndexStorage.GetIndexInstance(index);
                        isIndexStale = isIndexStale || (indexInstance != null && indexInstance.IsMapIndexingInProgress);
                    }

                    indexTimestamp = accessor.Staleness.IndexLastUpdatedAt(definition.IndexId);
                });
            var indexEtag = Database.Indexes.GetIndexEtag(index, null, indexQuery.ResultsTransformer);

            return GetMessageWithObject(
                new
                {
                    Count = results.Length,
                    Results = results,
                    Includes = new string[0],
                    IndexTimestamp = indexTimestamp.Item1,
                    IndexEtag = indexTimestamp.Item2,
                    TotalResults = totalResults.Value,
                    SkippedResults = 0,
                    NonAuthoritativeInformation = false,
                    ResultEtag = indexEtag,
                    IsStale = isIndexStale,
                    IndexName = index,
                    LastQueryTime = Database.IndexStorage.GetLastQueryTime(index)
                }, HttpStatusCode.OK, indexEtag);
        }

        private HttpResponseMessage GetIndexStats(string index)
        {
            IndexStats stats = null;
            Etag lastEtag = null;
            var instance = Database.IndexStorage.GetIndexInstance(index);
            Database.TransactionalStorage.Batch(accessor =>
            {
                stats = accessor.Indexing.GetIndexStats(instance.indexId);
                lastEtag = accessor.Staleness.GetMostRecentDocumentEtag();
            });

            if (stats == null)
                return GetEmptyMessage(HttpStatusCode.NotFound);

            stats.LastQueryTimestamp = Database.IndexStorage.GetLastQueryTime(instance.indexId);
            stats.SetLastDocumentEtag(lastEtag);
            return GetMessageWithObject(stats);
        }
    }
}
