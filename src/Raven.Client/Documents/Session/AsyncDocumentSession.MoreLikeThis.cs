//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public partial class AsyncDocumentSession
    {
        public Task<List<T>> MoreLikeThisAsync<T, TIndexCreator>(string documentId) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            if (documentId == null)
                throw new ArgumentNullException(nameof(documentId));

            var index = new TIndexCreator();
            return MoreLikeThisAsync<T>(new MoreLikeThisQuery { Query = CreateQuery(index.IndexName), DocumentId = documentId });
        }

        public Task<List<T>> MoreLikeThisAsync<T>(string index, string documentId)
        {
            return MoreLikeThisAsync<T>(new MoreLikeThisQuery
            {
                Query = CreateQuery(index),
                DocumentId = documentId,
            });
        }

        public async Task<List<T>> MoreLikeThisAsync<T>(MoreLikeThisQuery query)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var operation = new MoreLikeThisOperation(this, query);

            var command = operation.CreateRequest();
            await RequestExecutor.ExecuteAsync(command, Context, sessionId: _clientSessionId).ConfigureAwait(false);

            var result = command.Result;
            operation.SetResult(result);

            return operation.Complete<T>();
        }

        private static string CreateQuery(string indexName)
        {
            var fromToken = FromToken.Create(indexName, null);

            var sb = new StringBuilder();
            fromToken.WriteTo(sb);

            return sb.ToString();
        }
    }
}
