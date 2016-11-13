using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Raven.Abstractions.Data;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document.SessionOperations;
using Raven.NewClient.Client.Shard;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Document.Batches
{
    public class LazyLoadOperation<T> : ILazyOperation
    {
        private readonly LoadOperation loadOperation;
        private readonly string[] ids;
        private readonly string transformer;
        private readonly KeyValuePair<string, Type>[] includes;

        public LazyLoadOperation(
            LoadOperation loadOperation,
            string[] ids,
            KeyValuePair<string, Type>[] includes,
            string transformer = null)
        {
            this.loadOperation = loadOperation;
            this.ids = ids;
            this.includes = includes;
            this.transformer = transformer;
        }

        public LazyLoadOperation(
            LoadOperation loadOperation,
            string id)
        {
            this.loadOperation = loadOperation;
            ids = new[] { id };
        }

        public GetRequest CreateRequest()
        {
            var queryBuilder = new StringBuilder("?");
            includes.ApplyIfNotNull(include => queryBuilder.AppendFormat("&include={0}", include));
            ids.ApplyIfNotNull(id => queryBuilder.AppendFormat("&id={0}", Uri.EscapeDataString(id)));

            if (string.IsNullOrEmpty(transformer) == false)
                queryBuilder.AppendFormat("&transformer={0}", transformer);

            return new GetRequest
            {
                Url = "/docs",
                Query = queryBuilder.ToString()
            };
        }

        public object Result { get; set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; set; }

        public void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy)
        {
            var list = new List<LoadResult>(
                from response in responses
                let result = response.Result
                select new LoadResult
                {
                    Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
                    Results = result.Value<RavenJArray>("Results").Select(x => x as RavenJObject).ToList()
                });

            var capacity = list.Max(x => x.Results.Count);

            var finalResult = new LoadResult
            {
                Includes = new List<RavenJObject>(),
                Results = new List<RavenJObject>(Enumerable.Range(0, capacity).Select(x => (RavenJObject)null))
            };


            foreach (var multiLoadResult in list)
            {
                finalResult.Includes.AddRange(multiLoadResult.Includes);

                for (int i = 0; i < multiLoadResult.Results.Count; i++)
                {
                    if (finalResult.Results[i] == null)
                        finalResult.Results[i] = multiLoadResult.Results[i];
                }
            }
            RequiresRetry = loadOperation.SetResult(finalResult);
            if (RequiresRetry == false)
                Result = loadOperation.Complete<T>();

        }

        public void HandleResponse(GetResponse response)
        {
            if (response.ForceRetry)
            {
                Result = null;
                RequiresRetry = true;
                return;
            }

            var result = response.Result;

            var multiLoadResult = new LoadResult
            {
                Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
                Results = result.Value<RavenJArray>("Results").Select(x => x as RavenJObject).ToList()
            };
            HandleResponse(multiLoadResult);
        }

        private void HandleResponse(LoadResult loadResult)
        {
            RequiresRetry = loadOperation.SetResult(loadResult);
            if (RequiresRetry == false)
                Result = loadOperation.Complete<T>();
        }

        public IDisposable EnterContext()
        {
            return loadOperation.EnterLoadContext();
        }
    }
}
