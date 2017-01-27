//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.NewClient.Client.Commands;

namespace Raven.NewClient.Client.Document.Async
{
    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public partial class AsyncDocumentSession
    {
        public async Task<List<T>> GetRevisionsForAsync<T>(string id, int start = 0, int pageSize = 25)
        {
            var operation = new GetRevisionOperation(this, id, start, pageSize);

            var command = operation.CreateRequest();
            await RequestExecuter.ExecuteAsync(command, Context).ConfigureAwait(false);

            return operation.Complete<T>();
        }
    }
}
