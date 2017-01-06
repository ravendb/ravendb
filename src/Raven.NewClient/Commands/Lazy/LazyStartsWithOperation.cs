// -----------------------------------------------------------------------
//  <copyright file="LazyStartsWithOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document.Batches;
using Raven.NewClient.Client.Shard;
using Sparrow.Json;

namespace Raven.NewClient.Client.Commands.Lazy
{
   
    public class LazyStartsWithOperation<T> : ILazyOperation
    {
        private readonly string keyPrefix;

        private readonly string matches;

        private readonly string exclude;

        private readonly int start;

        private readonly int pageSize;

        private readonly InMemoryDocumentSessionOperations sessionOperations;

        private readonly RavenPagingInformation pagingInformation;
        private readonly string skipAfter;

        public LazyStartsWithOperation(string keyPrefix, string matches, string exclude, int start, int pageSize, InMemoryDocumentSessionOperations sessionOperations, RavenPagingInformation pagingInformation, string skipAfter)
        {
            this.keyPrefix = keyPrefix;
            this.matches = matches;
            this.exclude = exclude;
            this.start = start;
            this.pageSize = pageSize;
            this.sessionOperations = sessionOperations;
            this.pagingInformation = pagingInformation;
            this.skipAfter = skipAfter;
        }

        public GetRequest CreateRequest()
        {
            var actualStart = start;

            var nextPage = pagingInformation != null && pagingInformation.IsForPreviousPage(start, pageSize);
            if (nextPage)
                actualStart = pagingInformation.NextPageStart;

            return new GetRequest
            {
                Url = "/docs",
                Query =
                    string.Format(
                        "startsWith={0}&matches={3}&exclude={4}&start={1}&pageSize={2}&next-page={5}&skipAfter={6}",
                        Uri.EscapeDataString(keyPrefix),
                        actualStart,
                        pageSize,
                        Uri.EscapeDataString(matches ?? ""),
                        Uri.EscapeDataString(exclude ?? ""),
                        nextPage ? "true" : "false",
                        skipAfter)
            };
        }

        public object Result { get; set; }

        public QueryResult QueryResult { get; set; }

        public bool RequiresRetry { get; set; }

        public void HandleResponse(BlittableJsonReaderObject response)
        {
            throw new NotImplementedException();
            /*if (response.RequestHasErrors())
            {
                Result = null;
                RequiresRetry = false;
                return;
            }

            var jsonDocuments = SerializationHelper.RavenJObjectsToJsonDocuments(((RavenJArray)response.Result).OfType<RavenJObject>());

            int nextPageStart;
            if (pagingInformation != null && int.TryParse(response.Headers[Constants.Headers.NextPageStart], out nextPageStart))
                pagingInformation.Fill(start, pageSize, nextPageStart);

            Result = jsonDocuments
                .Select(sessionOperations.TrackEntity<T>)
                .ToArray();*/
        }
    }
}
