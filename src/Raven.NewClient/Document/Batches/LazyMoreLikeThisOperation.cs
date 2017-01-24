// -----------------------------------------------------------------------
//  <copyright file="LazyMoreLikeThisOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Json;
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
            var uri = query.GetRequestUri();
            var separator = uri.IndexOf('?');
            return new GetRequest()
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

            var res = JsonDeserializationClient.GetDocumentResult((BlittableJsonReaderObject)response.Result);
            _loadOperation.SetResult(res);
            Result = _loadOperation.GetDocuments<T>();
        }
    }
}
