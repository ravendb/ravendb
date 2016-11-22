// -----------------------------------------------------------------------
//  <copyright file="LazyMoreLikeThisOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Shard;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Document.Batches
{
    public class LazyMoreLikeThisOperation<T> : ILazyOperation
    {
        private readonly LoadOperation _loadOperation;
        private readonly MoreLikeThisQuery query;

        public LazyMoreLikeThisOperation(LoadOperation loadOperation, MoreLikeThisQuery query)
        {
            _loadOperation = loadOperation;
            this.query = query;
        }

        public GetRequest CreateRequest()
        {
            var uri = query.GetRequestUri();

            var separator = uri.IndexOf('?');

            return new GetRequest()
            {
                Url = uri.Substring(0, separator),
                Query = uri.Substring(separator + 1, uri.Length - separator - 1)
            };
        }

        public object Result { get; private set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; private set; }

        public void HandleResponse(GetResponse response)
        {
            var result = response.Result;

            var multiLoadResult = new LoadResult
            {
                Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
                Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
            };

            HandleResponse(multiLoadResult);
        }

        private void HandleResponse(LoadResult loadResult)
        {
            throw new NotImplementedException();
            /* RequiresRetry = _loadOperation.SetResult(loadResult);
             if (RequiresRetry == false)
                 Result = _loadOperation.Complete<T>();*/
        }

        public void HandleResponses(GetResponse[] responses, ShardStrategy shardStrategy)
        {
            throw new NotImplementedException();
            /* var list = new List<LoadResult>(
                 from response in responses
                 let result = response.Result
                 select new LoadResult
                 {
                     Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
                     Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
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
             RequiresRetry = _loadOperation.SetResult(finalResult);
             if (RequiresRetry == false)
                 Result = _loadOperation.Complete<T>();*/
        }

        public IDisposable EnterContext()
        {
            throw new NotImplementedException();
            /*return _loadOperation.EnterLoadContext();*/
        }
    }
}
