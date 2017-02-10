// -----------------------------------------------------------------------
//  <copyright file="LazyMoreLikeThisOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Client.Commands;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Document.Batches
{
    public class LazyMoreLikeThisOperation<T> : ILazyOperation
    {
        private readonly MoreLikeThisQuery _query;
        private readonly MoreLikeThisOperation _operation;

        public LazyMoreLikeThisOperation(InMemoryDocumentSessionOperations session, MoreLikeThisQuery query)
        {
            _query = query;
            _operation = new MoreLikeThisOperation(session, query);
        }

        public GetRequest CreateRequest()
        {
            var uri = _query.GetRequestUri();
            var separator = uri.IndexOf('?');
            return new GetRequest
            {
                Url = uri.Substring(0, separator),
                Query = uri.Substring(separator, uri.Length - separator - 1)
            };
        }

        public object Result { get; private set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; private set; }

        public void HandleResponse(GetResponse response)
        {
            if (response == null)
            {
                Result = null;
                return;
            }

            if (response.ForceRetry)
            {
                Result = null;
                RequiresRetry = true;
                return;
            }

            var result = JsonDeserializationClient.MoreLikeThisQueryResult((BlittableJsonReaderObject)response.Result);
            _operation.SetResult(result);

            Result = _operation.Complete<T>();
        }
    }
}
