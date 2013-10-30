//-----------------------------------------------------------------------
// <copyright file="Index.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using System.Linq;
using Raven.Abstractions.Logging;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Queries;
using Raven.Database.Server.Abstractions;
using Raven.Database.Storage;

namespace Raven.Database.Server.Responders
{
    public class Index : AbstractRequestResponder
    {
        public override string UrlPattern
        {
            get { return @"^/indexes/(.+)"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET", "PUT", "POST", "DELETE", "HEAD", "RESET" }; }
        }

        public override void Respond(IHttpContext context)
        {
            var match = urlMatcher.Match(context.GetRequestUrl());
            var index = match.Groups[1].Value;

            switch (context.Request.HttpMethod)
            {
                case "HEAD":
                    if (Database.IndexDefinitionStorage.IndexNames.Contains(index, StringComparer.OrdinalIgnoreCase) == false)
                        context.SetStatusToNotFound();
                    break;
                case "GET":
                    OnGet(context, index);
                    break;
                case "POST":
                    OnPost(context, index);
                    break;
                case "PUT":
                    Put(context, index);
                    break;
                case "RESET":
                    Database.ResetIndex(index);
                    context.WriteJson(new { Reset = index });
                    break;
                case "DELETE":
                    context.SetStatusToDeleted();
                    Database.DeleteIndex(index);
                    break;
            }
        }

        private void OnPost(IHttpContext context, string index)
        {
            if ("forceWriteToDisk".Equals(context.Request.QueryString["op"], StringComparison.InvariantCultureIgnoreCase))
            {
                Database.IndexStorage.ForceWriteToDisk(index);
            }
            else if ("lockModeChange".Equals(context.Request.QueryString["op"], StringComparison.InvariantCultureIgnoreCase))
            {
                HandleIndexLockModeChange(context, index);
            }
            else
            {
                context.SetStatusToBadRequest();
                context.WriteJson(new
                {
                    Error = "Not idea how to handle a POST on " + index + " with op=" + (context.Request.QueryString["op"] ?? "<no val specified>")
                });
            }
        }

        private void HandleIndexLockModeChange(IHttpContext context, string index)
        {
            var lockModeStr = context.Request.QueryString["mode"];

            IndexLockMode indexLockMode;
            if (Enum.TryParse(lockModeStr, out indexLockMode) == false)
            {
                context.SetStatusToBadRequest();
                context.WriteJson(new
                {
                    Error = "Cannot understand index lock mode: " + lockModeStr
                });
                return;
            }

            var indexDefinition = Database.IndexDefinitionStorage.GetIndexDefinition(index);
            if (indexDefinition == null)
            {
                context.SetStatusToNotFound();
                context.WriteJson(new
                {
                    Error = "Cannot find index : " + index
                });
                return;
            }
            var definition = indexDefinition.Clone();
            definition.LockMode = indexLockMode;
            Database.IndexDefinitionStorage.UpdateIndexDefinitionWithoutUpdatingCompiledIndex(definition);
        }

        private void Put(IHttpContext context, string index)
        {
            var data = context.ReadJsonObject<IndexDefinition>();
            if (data == null || (data.Map == null && (data.Maps == null || data.Maps.Count == 0)))
            {
                context.SetStatusToBadRequest();
                context.WriteJson(new { Error = "Expected json document with 'Map' or 'Maps' property" });
                return;
            }

            try
            {
                Database.PutIndex(index, data);
                context.SetStatusToCreated("/indexes/" + Uri.EscapeUriString(index));
                context.WriteJson(new {Index = index});
            }
            catch (Exception ex)
            {
                var compilationException = ex as IndexCompilationException;
                
                context.SetStatusToBadRequest();
                context.WriteJson(new
                {
                    Message = ex.Message, 
                    IndexDefinitionProperty=compilationException != null ? compilationException.IndexDefinitionProperty : "",
                    ProblematicText = compilationException != null ? compilationException.ProblematicText : "",
                    Error = ex.ToString()
                });
            }
        }

        private void OnGet(IHttpContext context, string index)
        {
            if (string.IsNullOrEmpty(context.Request.QueryString["definition"]) == false)
            {
                GetIndexDefinition(context, index);
            }
            else if (string.IsNullOrEmpty(context.Request.QueryString["source"]) == false)
            {
                GetIndexSource(context, index);
            }
            else if (string.IsNullOrEmpty(context.Request.QueryString["debug"]) == false)
            {
                DebugIndex(context, index);
            }
            else if (string.IsNullOrEmpty(context.Request.QueryString["explain"]) == false)
            {
                GetExplanation(context, index);
            }
            else
            {
                GetIndexQueryResult(context, index);
            }
        }

        private void DebugIndex(IHttpContext context, string index)
        {
            switch (context.Request.QueryString["debug"].ToLowerInvariant())
            {
                case "map":
                    GetIndexMappedResult(context, index);
                    break;
                case "reduce":
                    GetIndexReducedResult(context, index);
                    break;
                case "schedules":
                    GetIndexScheduledReduces(context, index);
                    break;
                case "keys":
                    GetIndexKeysStats(context, index);
                    break;
                case "entries":
                    GetIndexEntries(context, index);
                    break;
                case "stats":
                    GetIndexStats(context, index);
                    break;
                default:
                    context.WriteJson(new
                    {
                        Error = "Unknown debug option " + context.Request.QueryString["debug"]
                    });
                    context.SetStatusToBadRequest();
                    break;
            }
        }

        private void GetIndexScheduledReduces(IHttpContext context, string index)
        {
            List<ScheduledReductionDebugInfo> mappedResult = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                var instance = Database.IndexStorage.GetIndexInstance(index);
                mappedResult = accessor.MapReduce.GetScheduledReductionForDebug(instance.indexId, context.GetStart(), context.GetPageSize(Settings.MaxPageSize))
                    .ToList();
            });
            context.WriteJson(new
            {
                mappedResult.Count,
                Results = mappedResult
            });
        }

        private void GetIndexKeysStats(IHttpContext context, string index)
        {
            var definition = Database.IndexDefinitionStorage.GetIndexDefinition(index);
            if (definition == null) {
                context.SetStatusToNotFound();
                return;
            }

            List<ReduceKeyAndCount> keys = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                keys = accessor.MapReduce.GetKeysStats(definition.IndexId,
                         context.GetStart(),
                         context.GetPageSize(Database.Configuration.MaxPageSize))
                    .ToList();
            });
            context.WriteJson(new
            {
                keys.Count,
                Results = keys
            });
        }

        private void GetIndexStats(IHttpContext context, string index)
        {
            IndexStats stats = null;
            var instance = Database.IndexStorage.GetIndexInstance(index);
            Database.TransactionalStorage.Batch(accessor =>
            {
                stats = accessor.Indexing.GetIndexStats(instance.indexId);
            });

            if (stats == null)
            {
                context.SetStatusToNotFound();
                return;
            }

            stats.LastQueryTimestamp = Database.IndexStorage.GetLastQueryTime(instance.indexId);
            stats.Performance = Database.IndexStorage.GetIndexingPerformance(instance.indexId);

            context.WriteJson(stats);
        }

        private void GetIndexEntries(IHttpContext context, string index)
        {
            var indexQuery = context.GetIndexQueryFromHttpContext(Database.Configuration.MaxPageSize);
            var totalResults = new Reference<int>();

            var isDynamic = index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase)
                            || index.Equals("dynamic", StringComparison.OrdinalIgnoreCase);

            if (isDynamic)
            {
                GetIndexEntriesForDynamicIndex(context, index, indexQuery, totalResults);
            }
            else
            {
                GetIndexEntriesForExistingIndex(context, index, indexQuery, totalResults);
            }
        }

        private void GetIndexEntriesForDynamicIndex(IHttpContext context, string index, IndexQuery indexQuery, Reference<int> totalResults)
        {
            string entityName;
            var dynamicIndexName = GetDynamicIndexName(index, indexQuery, out entityName);

            if (dynamicIndexName == null)
            {
                context.SetStatusToNotFound();
                return;
            }
            GetIndexEntriesForExistingIndex(context, dynamicIndexName, indexQuery, totalResults);
        }

        private void GetIndexEntriesForExistingIndex(IHttpContext context, string index, IndexQuery indexQuery, Reference<int> totalResults)
        {
            var results = Database
                    .IndexStorage
                    .IndexEntires(index, indexQuery, Database.IndexQueryTriggers, totalResults)
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
            var indexEtag = Database.GetIndexEtag(index, null, indexQuery.ResultsTransformer);
            context.WriteETag(indexEtag);
            context.WriteJson(
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
                });
        }

        private void GetExplanation(IHttpContext context, string index)
        {
            var dynamicIndex = index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) ||
                               index.Equals("dynamic", StringComparison.OrdinalIgnoreCase);

            if (dynamicIndex == false)
            {
                context.SetStatusToBadRequest();
                context.WriteJson(new
                                {
                                    Error = "Explain can only work on dynamic indexes"
                                });
                return;
            }

            var indexQuery = context.GetIndexQueryFromHttpContext(Database.Configuration.MaxPageSize);
            string entityName = null;
            if (index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase))
                entityName = index.Substring("dynamic/".Length);

            var explanations = Database.ExplainDynamicIndexSelection(entityName, indexQuery);

            context.WriteJson(explanations);
        }

        private void GetIndexMappedResult(IHttpContext context, string index)
        {
            var definition = Database.IndexDefinitionStorage.GetIndexDefinition(index);
            if (definition == null)
            {
                context.SetStatusToNotFound();
                return;
            }
            var key = context.Request.QueryString["key"];
            if (string.IsNullOrEmpty(key))
            {
                List<string> keys = null;
                Database.TransactionalStorage.Batch(accessor =>
                {
                    keys = accessor.MapReduce.GetKeysForIndexForDebug(definition.IndexId, context.GetStart(), context.GetPageSize(Settings.MaxPageSize))
                        .ToList();
                });

                context.WriteJson(new
                {
                    Error = "Query string argument \'key\' is required",
                    Keys = keys
                });
                context.SetStatusToBadRequest();
                return;
            }

            List<MappedResultInfo> mappedResult = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                mappedResult = accessor.MapReduce.GetMappedResultsForDebug(definition.IndexId, key, context.GetStart(), context.GetPageSize(Settings.MaxPageSize))
                    .ToList();
            });
            context.WriteJson(new
            {
                mappedResult.Count,
                Results = mappedResult
            });
        }

        private void GetIndexReducedResult(IHttpContext context, string index)
        {
            var definition = Database.IndexDefinitionStorage.GetIndexDefinition(index);
            if(definition == null)
            {
                context.SetStatusToNotFound();
                return;
            }
            var key = context.Request.QueryString["key"];
            if (string.IsNullOrEmpty(key))
            {
                context.WriteJson(new
                {
                    Error = "Query string argument 'key' is required"
                });
                context.SetStatusToBadRequest();
                return;
            }

            int level;
            if (int.TryParse(context.Request.QueryString["level"], out level) == false || (level != 1 && level != 2))
            {
                context.WriteJson(new
                {
                    Error = "Query string argument 'level' is required and must be 1 or 2"
                });
                context.SetStatusToBadRequest();
                return;
            }

            List<MappedResultInfo> mappedResult = null;
            Database.TransactionalStorage.Batch(accessor =>
            {
                mappedResult = accessor.MapReduce.GetReducedResultsForDebug(definition.IndexId, key, level, context.GetStart(), context.GetPageSize(Settings.MaxPageSize))
                    .ToList();
            });
            context.WriteJson(new
            {
                mappedResult.Count,
                Results = mappedResult
            });
        }

        private void GetIndexQueryResult(IHttpContext context, string index)
        {
            Etag indexEtag;
            var queryResult = ExecuteQuery(context, index, out indexEtag);

            if (queryResult == null)
                return;

            var includes = context.Request.QueryString.GetValues("include") ?? new string[0];
            var loadedIds = new HashSet<string>(
                queryResult.Results
                    .Where(x => x["@metadata"] != null)
                    .Select(x => x["@metadata"].Value<string>("@id"))
                    .Where(x => x != null)
                );
            var command = new AddIncludesCommand(Database, GetRequestTransaction(context),
                                                 (etag, doc) => queryResult.Includes.Add(doc), includes, loadedIds);
            foreach (var result in queryResult.Results)
            {
                command.Execute(result);
            }
            command.AlsoInclude(queryResult.IdsToInclude);

            context.WriteETag(indexEtag);
            if (queryResult.NonAuthoritativeInformation)
                context.SetStatusToNonAuthoritativeInformation();

            context.WriteJson(queryResult);
        }

        private void GetIndexDefinition(IHttpContext context, string index)
        {
            var indexDefinition = Database.GetIndexDefinition(index);
            if (indexDefinition == null)
            {
                context.SetStatusToNotFound();
                return;
            }

            indexDefinition.Fields = Database.GetIndexFields(index);

            context.WriteJson(new
            {
                Index = indexDefinition,
            });
        }

        private void GetIndexSource(IHttpContext context, string index)
        {
            var viewGenerator = Database.IndexDefinitionStorage.GetViewGenerator(index);
            if (viewGenerator == null)
            {
                context.SetStatusToNotFound();
                return;
            }

            context.Write(viewGenerator.SourceCode);
        }

        private QueryResultWithIncludes ExecuteQuery(IHttpContext context, string index, out Etag indexEtag)
        {
            var indexQuery = context.GetIndexQueryFromHttpContext(Database.Configuration.MaxPageSize);
            RewriteDateQueriesFromOldClients(context, indexQuery);

            var sp = Stopwatch.StartNew();
            var result = index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) || index.Equals("dynamic", StringComparison.OrdinalIgnoreCase) ?
                PerformQueryAgainstDynamicIndex(context, index, indexQuery, out indexEtag) :
                PerformQueryAgainstExistingIndex(context, index, indexQuery, out indexEtag);

            sp.Stop();

            context.Log(log => log.Debug(() =>
            {
                var sb = new StringBuilder("\tQuery: ")
                    .Append(indexQuery.Query)
                    .AppendLine();
                sb.Append("\t").AppendFormat("Time: {0:#,#;;0} ms", sp.ElapsedMilliseconds).AppendLine();

                if (result == null)
                    return sb.ToString();

                sb.Append("\tIndex: ")
                    .AppendLine(result.IndexName);
                sb.Append("\t").AppendFormat("Results: {0:#,#;;0} returned out of {1:#,#;;0} total.", result.Results.Count, result.TotalResults).AppendLine();

                return sb.ToString();
            }));

            return result;
        }

        static Regex oldDateTimeFormat = new Regex(@"(\:|\[|{|TO\s) \s* (\d{17})", RegexOptions.Compiled | RegexOptions.IgnorePatternWhitespace);

        private void RewriteDateQueriesFromOldClients(IHttpContext context, IndexQuery indexQuery)
        {
            var clientVersion = context.Request.Headers["Raven-Client-Version"];
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
				var newDateTimeFormat = time.ToString(Default.DateTimeFormatsToWrite, CultureInfo.InvariantCulture);
                builder.Insert(matches[i].Groups[2].Index, newDateTimeFormat);
            }
            indexQuery.Query = builder.ToString();
        }

        private QueryResultWithIncludes PerformQueryAgainstExistingIndex(IHttpContext context, string index, IndexQuery indexQuery, out Etag indexEtag)
        {
            indexEtag = Database.GetIndexEtag(index, null, indexQuery.ResultsTransformer);
            if (context.MatchEtag(indexEtag))
            {
                Database.IndexStorage.MarkCachedQuery(index);
                context.SetStatusToNotModified();
                return null;
            }

            var queryResult = Database.Query(index, indexQuery);
            indexEtag = Database.GetIndexEtag(index, queryResult.ResultEtag, indexQuery.ResultsTransformer);
            return queryResult;
        }

        private QueryResultWithIncludes PerformQueryAgainstDynamicIndex(IHttpContext context, string index, IndexQuery indexQuery, out Etag indexEtag)
        {
            string entityName;
            var dynamicIndexName = GetDynamicIndexName(index, indexQuery, out entityName);

            if (dynamicIndexName != null && Database.IndexStorage.HasIndex(dynamicIndexName))
            {
                indexEtag = Database.GetIndexEtag(dynamicIndexName, null, indexQuery.ResultsTransformer);
                if (context.MatchEtag(indexEtag))
                {
                    Database.IndexStorage.MarkCachedQuery(dynamicIndexName);
                    context.SetStatusToNotModified();
                    return null;
                }
            }

            if (dynamicIndexName == null && // would have to create a dynamic index
                Database.Configuration.CreateAutoIndexesForAdHocQueriesIfNeeded == false) // but it is disabled
            {
                indexEtag = Etag.InvalidEtag;
                var explanations = Database.ExplainDynamicIndexSelection(entityName, indexQuery);
                context.SetStatusToBadRequest();
                var target = entityName == null ? "all documents" : entityName + " documents";
                context.WriteJson(new
                                    {
                                        Error = "Executing the query " + indexQuery.Query + " on " + target + " require creation of temporary index, and it has been explicitly disabled.",
                                        Explanations = explanations
                                    });
                return null;
            }

            var queryResult = Database.ExecuteDynamicQuery(entityName, indexQuery);

            // have to check here because we might be getting the index etag just 
            // as we make a switch from temp to auto, and we need to refresh the etag
            // if that is the case. This can also happen when the optimizer
            // decided to switch indexes for a query.
            indexEtag = (dynamicIndexName == null || queryResult.IndexName == dynamicIndexName)
                            ? Database.GetIndexEtag(queryResult.IndexName, queryResult.ResultEtag, indexQuery.ResultsTransformer)
                            : Etag.InvalidEtag;

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
    }
}
