// -----------------------------------------------------------------------
//  <copyright file="LazyStartsWithOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Queries;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazyStartsWithOperation<T> : ILazyOperation
    {
        private readonly string _idPrefix;

        private readonly string _matches;

        private readonly string _exclude;

        private readonly int _start;

        private readonly int _pageSize;

        private readonly InMemoryDocumentSessionOperations _sessionOperations;

        private readonly string _startAfter;

        public LazyStartsWithOperation(string idPrefix, string matches, string exclude, int start, int pageSize, InMemoryDocumentSessionOperations sessionOperations, string startAfter)
        {
            _idPrefix = idPrefix;
            _matches = matches;
            _exclude = exclude;
            _start = start;
            _pageSize = pageSize;
            _sessionOperations = sessionOperations;
            _startAfter = startAfter;
        }

        public GetRequest CreateRequest(JsonOperationContext ctx)
        {
            return new GetRequest
            {
                Url = "/docs",
                Query = string.Format(
                        "?startsWith={0}&matches={3}&exclude={4}&start={1}&pageSize={2}&startAfter={5}",
                        Uri.EscapeDataString(_idPrefix),
                        _start,
                        _pageSize,
                        Uri.EscapeDataString(_matches ?? ""),
                        Uri.EscapeDataString(_exclude ?? ""),
                        _startAfter)
            };
        }

        public object Result { get; set; }

        public QueryResult QueryResult { get; set; }

        public bool RequiresRetry { get; set; }

        public void HandleResponse(GetResponse response)
        {
            var getDocumentResult = JsonDeserializationClient.GetDocumentsResult((BlittableJsonReaderObject)response.Result);

            var finalResults = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
            foreach (BlittableJsonReaderObject document in getDocumentResult.Results)
            {
                var newDocumentInfo = DocumentInfo.GetNewDocumentInfo(document);
                _sessionOperations.DocumentsById.Add(newDocumentInfo);
                if (newDocumentInfo.Id == null)
                    continue; // is this possible?

                if (_sessionOperations.IsDeleted(newDocumentInfo.Id))
                {
                    finalResults[newDocumentInfo.Id] = default(T);
                    continue;
                }
                DocumentInfo doc;
                if (_sessionOperations.DocumentsById.TryGetValue(newDocumentInfo.Id, out doc))
                {
                    finalResults[newDocumentInfo.Id] = _sessionOperations.TrackEntity<T>(doc);
                    continue;
                }

                finalResults[newDocumentInfo.Id] = default(T);
            }

            Result = finalResults;
        }
    }
}
