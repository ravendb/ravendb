using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Client.Connection;
using Raven.Client.Data;

namespace Raven.Client.Document.SessionOperations
{
    public class LoadOperation
    {
        private readonly static ILog log = LogManager.GetLogger(typeof(LoadOperation));

        private readonly InMemoryDocumentSessionOperations sessionOperations;
        internal Func<IDisposable> disableAllCaching { get; set; }
        private readonly string[] ids;
        private readonly KeyValuePair<string, Type>[] includes;
        bool firstRequest = true;
        JsonDocument[] results;
        JsonDocument[] includeResults;

        public LoadOperation(InMemoryDocumentSessionOperations sessionOperations, Func<IDisposable> disableAllCaching, string[] ids, KeyValuePair<string, Type>[] includes = null)
        {
            this.sessionOperations = sessionOperations;
            this.disableAllCaching = disableAllCaching;
            this.ids = ids;
            this.includes = includes;
        }

        public LoadOperation(InMemoryDocumentSessionOperations sessionOperations, Func<IDisposable> disableAllCaching, string id) 
            : this(sessionOperations, disableAllCaching, new [] {id}, null)
        {
        }

        public void LogOperation()
        {
            if (ids == null)
                return;
            if (log.IsDebugEnabled)
                log.Debug("Bulk loading ids [{0}] from {1}", string.Join(", ", ids), sessionOperations.StoreIdentifier);
        }

        public IDisposable EnterLoadContext()
        {
            if (firstRequest == false) // if this is a repeated request, we mustn't use the cached result, but have to re-query the server
                return disableAllCaching();

            return null;
        }

        public bool SetResult(JsonDocument document)
        {
            firstRequest = false;
            includeResults = new JsonDocument[0];
            results = new[] {document};

            return false;
        }


        public bool SetResult(LoadResult loadResult)
        {
            firstRequest = false;
            includeResults = SerializationHelper.RavenJObjectsToJsonDocuments(loadResult.Includes).ToArray();
            results = SerializationHelper.RavenJObjectsToJsonDocuments(loadResult.Results).ToArray();

            return false;
        }

        public T[] Complete<T>()
        {
            for (var i = 0; i < includeResults.Length; i++)
            {
                var include = includeResults[i];
                sessionOperations.TrackIncludedDocument(include);
            }

            var finalResults = ids != null ?
                ReturnResultsById<T>() :
                ReturnResults<T>();
            for (var i = 0; i < finalResults.Length; i++)
            {
                var finalResult = finalResults[i];
                if (ReferenceEquals(finalResult, null))
                    sessionOperations.RegisterMissing(ids[i]);
            }

            var includePaths = includes != null ? includes.Select(x => x.Key).ToArray() : null;
            sessionOperations.RegisterMissingIncludes(results.Where(x => x != null).Select(x => x.DataAsJson), includePaths);

            return finalResults;
        }

        private T[] ReturnResults<T>()
        {
            var finalResults = new T[results.Length];
            for (int i = 0; i < results.Length; i++)
            {
                if (results[i] != null)
                    finalResults[i] = sessionOperations.TrackEntity<T>(results[i]);
            }
            return finalResults;
        }

        private T[] ReturnResultsById<T>()
        {
            var finalResults = new T[ids.Length];
            var dic = new Dictionary<string, FinalResultPositionById>(ids.Length, StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < ids.Length; i++)
            {
                if (ids[i] == null)
                    continue;

                FinalResultPositionById position;
                if (dic.TryGetValue(ids[i], out position) == false)
                {
                    dic[ids[i]] = new FinalResultPositionById
                    {
                        SingleReturn = i
                    };
                }
                else
                {
                    if (position.SingleReturn != null)
                    {
                        position.MultipleReturns = new List<int>(2)
                        {
                            position.SingleReturn.Value
                        };

                        position.SingleReturn = null;
                    }

                    position.MultipleReturns.Add(i);
                }  
            }

            foreach (var jsonDocument in results)
            {
                if (jsonDocument == null)
                    continue;

                var id = jsonDocument.Metadata.Value<string>("@id");

                if (id == null)
                    continue;

                FinalResultPositionById position;

                if (dic.TryGetValue(id, out position))
                {
                    if (position.SingleReturn != null)
                    {
                        finalResults[position.SingleReturn.Value] = sessionOperations.TrackEntity<T>(jsonDocument);
                    }
                    else if (position.MultipleReturns != null)
                    {
                        T trackedEntity = sessionOperations.TrackEntity<T>(jsonDocument);

                        foreach (var pos in position.MultipleReturns)
                        {
                            finalResults[pos] = trackedEntity;
                        }
                    }
                }
            }

            return finalResults;
        }

        private class FinalResultPositionById
        {
            public int? SingleReturn;

            public List<int> MultipleReturns;
        }
    }
}
