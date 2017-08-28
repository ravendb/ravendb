//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        public List<T> MoreLikeThis<T, TIndexCreator>(string documentId) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            if (documentId == null)
                throw new ArgumentNullException(nameof(documentId));

            var index = new TIndexCreator();
            return MoreLikeThis<T>(new MoreLikeThisQuery { Query = CreateQuery(index.IndexName), DocumentId = documentId });
        }

        public List<T> MoreLikeThis<T>(string index, string documentId)
        {
            if (index == null) throw new ArgumentNullException(nameof(index));
            if (documentId == null)
                throw new ArgumentNullException(nameof(documentId));

            return MoreLikeThis<T>(new MoreLikeThisQuery { Query = CreateQuery(index), DocumentId = documentId });
        }

        public List<T> MoreLikeThis<T>(MoreLikeThisQuery query)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            var operation = new MoreLikeThisOperation(this, query);

            var command = operation.CreateRequest();
            RequestExecutor.Execute(command, Context, sessionId: _clientSessionId);

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
