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
using Sparrow.Json;


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
            throw new NotImplementedException();

            /*var uri = query.GetRequestUri();

            var separator = uri.IndexOf('?');

            return new GetRequest()
            {
                Url = uri.Substring(0, separator),
                Query = uri.Substring(separator + 1, uri.Length - separator - 1)
            };*/
        }

        public object Result { get; private set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; private set; }

        public void HandleResponse(BlittableJsonReaderObject response)
        {
            throw new NotImplementedException();

            /*var result = response.Result;

            var multiLoadResult = new LoadResult
            {
                Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
                Results = result.Value<RavenJArray>("Results").Cast<RavenJObject>().ToList()
            };

            HandleResponse(multiLoadResult);*/
        }
    }
}
