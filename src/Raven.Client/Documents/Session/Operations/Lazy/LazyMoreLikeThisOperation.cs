// -----------------------------------------------------------------------
//  <copyright file="LazyMoreLikeThisOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net.Http;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Extensions;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazyMoreLikeThisOperation<T> : ILazyOperation
    {
        private readonly MoreLikeThisQuery _query;
        private readonly MoreLikeThisOperation _operation;
        private DocumentConventions _conventions;

        public LazyMoreLikeThisOperation(InMemoryDocumentSessionOperations session, MoreLikeThisQuery query)
        {
            if (session == null)
                throw new ArgumentNullException(nameof(session));

            _query = query ?? throw new ArgumentNullException(nameof(query));
            _conventions = session.Conventions;
            _operation = new MoreLikeThisOperation(session, query);
        }

        public GetRequest CreateRequest()
        {
            return new GetRequest
            {
                Url = "/queries?op=facets",
                Method = HttpMethod.Post,
                Content = new MoreLikeThisQueryContent(_conventions, _query)
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

        private class MoreLikeThisQueryContent : GetRequest.IContent
        {
            private readonly DocumentConventions _conventions;
            private readonly MoreLikeThisQuery _query;

            public MoreLikeThisQueryContent(DocumentConventions conventions, MoreLikeThisQuery query)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _query = query ?? throw new ArgumentNullException(nameof(query));
            }

            public void WriteContent(BlittableJsonTextWriter writer, JsonOperationContext context)
            {
                writer.WriteMoreLikeThisQuery(_conventions, context, _query);
            }
        }
    }
}
