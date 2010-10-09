using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;

namespace Raven.Database
{
    public class QueryRunner : MarshalByRefObject
    {
        private IRemoteStorage remoteStorage;

        private readonly ConcurrentDictionary<string, AbstractViewGenerator> queryCache =
            new ConcurrentDictionary<string, AbstractViewGenerator>();

        public override object InitializeLifetimeService()
        {
            return null;
        }

        public int QueryCacheSize
        {
            get { return queryCache.Count; }
        }

        public void Initialize(Type remoteStorageType, object state)
        {
            remoteStorage = (IRemoteStorage)Activator.CreateInstance(remoteStorageType, state);
        }

        public RemoteQueryResults Query(LinearQuery query)
        {
            var viewGenerator = queryCache.GetOrAdd(query.Query,
                                                            s =>
                                                            new DynamicViewCompiler("query", new IndexDefinition {Map = query.Query,},
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

                if(string.IsNullOrEmpty(viewGenerator.ForEntityName) == false) //optimization
                {
                    matchingDocs =
                        matchingDocs.Where(x => x.Item1.Metadata.Value<string>("Raven-Entity-Name") == viewGenerator.ForEntityName);
                }

                var docs = matchingDocs
                    .Select(x =>
                    {
                        lastResult = x.Item2;
                        return new DynamicJsonObject(x.Item1.ToJson());   
                    });

                results.AddRange(
                    RobustEnumeration(docs, viewGenerator.MapDefinition, errors)
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
      
        protected IEnumerable<object> RobustEnumeration(IEnumerable<object> input, IndexingFunc func, ICollection<string> errors)
        {
            var wrapped = new StatefulEnumerableWrapper<dynamic>(input.GetEnumerator());
            IEnumerator<object> en = func(wrapped).GetEnumerator();
            do
            {
                var moveSuccessful = MoveNext(en, wrapped, errors);
                if (moveSuccessful == false)
                    yield break;
                if (moveSuccessful == true)
                    yield return en.Current;
                else
                    en = func(wrapped).GetEnumerator();
            } while (true);
        }

        private static bool? MoveNext(IEnumerator en, StatefulEnumerableWrapper<object> innerEnumerator, ICollection<string> errors)
        {
            try
            {
                return en.MoveNext();
            }
            catch (Exception e)
            {
                errors.Add(String.Format("Doc '{0}', Error: {1}", TryGetDocKey(innerEnumerator.Current), e.Message));
            }
            return null;
        }

        private static string TryGetDocKey(object current)
        {
            var dic = current as DynamicJsonObject;
            if (dic == null)
                return null;
            var value = dic.GetValue("__document_id");
            if (value == null)
                return null;
            return value.ToString();
        }
    }

    [Serializable]
    public class RemoteQueryResults
    {
        public string[] Results { get; set; }
        public string[] Errors { get; set; }
        public int QueryCacheSize { get; set; }
        public int LastScannedResult { get; set; }
        public int TotalResults { get; set; }
    }
}