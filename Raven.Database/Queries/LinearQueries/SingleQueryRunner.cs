//-----------------------------------------------------------------------
// <copyright file="SingleQueryRunner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;

namespace Raven.Database.Queries.LinearQueries
{
    public class SingleQueryRunner : MarshalByRefObject,IRemoteSingleQueryRunner
    {
        private readonly ConcurrentDictionary<string, AbstractViewGenerator> queryCache;

        private readonly IRemoteStorage remoteStorage;

        public SingleQueryRunner(IRemoteStorage remoteStorage, ConcurrentDictionary<string, AbstractViewGenerator> queryCache)
        {
            this.remoteStorage = remoteStorage;
            this.queryCache = queryCache;
        }


        public RemoteQueryResults Query(LinearQuery query)
        {
            var viewGenerator = queryCache.GetOrAdd(query.Query,
                                                    s =>
                                                    new DynamicViewCompiler("query", new IndexDefinition { Map = query.Query, },
                                                                            new AbstractDynamicCompilationExtension[0])
                                                    {
                                                        RequiresSelectNewAnonymousType = false
                                                    }.GenerateInstance());

            var results = new List<string>();
            var errors = new List<string>();
            int lastResult = 0;
            int finalResult = 0;
            remoteStorage.Batch(actions =>
            {
                var firstAndLastDocumentIds = actions.Documents.FirstAndLastDocumentIds();
                finalResult = firstAndLastDocumentIds.Item2;
                var start = Math.Max(firstAndLastDocumentIds.Item1, query.Start);
                var matchingDocs = actions.Documents.DocumentsById(start, firstAndLastDocumentIds.Item2);

                if (string.IsNullOrEmpty(viewGenerator.ForEntityName) == false) //optimization
                {
                    matchingDocs =
                        matchingDocs.Where(x => x.Item1.Metadata.Value<string>("Raven-Entity-Name") == viewGenerator.ForEntityName);
                }

                var docs = matchingDocs
                    .Select(x=>
                    {
                        DocumentRetriever.EnsureIdInMetadata(x.Item1);
                        return x;
                    })
                    .Select(x =>
                    {
                        lastResult = x.Item2;
                        return new DynamicJsonObject(x.Item1.ToJson());
                    });

                var robustEnumerator = new RobustEnumerator
                {
                    OnError =
                        (exception, o) =>
                        errors.Add(String.Format("Doc '{0}', Error: {1}", Index.TryGetDocKey(o),
                                                 exception.Message))
                };
                results.AddRange(
                    robustEnumerator.RobustEnumeration(docs, viewGenerator.MapDefinition)
                        .Take(query.PageSize)
                        .Select(result => JsonExtensions.ToJObject(result).ToString())
                    );
            });

            return new RemoteQueryResults
            {
                LastScannedResult = lastResult,
                TotalResults = finalResult,
                Errors = errors.ToArray(),
                QueryCacheSize = queryCache.Count,
                Results = results.ToArray()
            };
        }


        public void Dispose()
        {
            remoteStorage.Dispose();
        }
    }
}
