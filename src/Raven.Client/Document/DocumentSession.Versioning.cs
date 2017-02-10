//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Client.Commands;

namespace Raven.Client.Document
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        public List<T> GetRevisionsFor<T>(string id, int start = 0, int pageSize = 25)
        {
            var operation = new GetRevisionOperation(this, id, start, pageSize);

            var command = operation.CreateRequest();
            RequestExecuter.Execute(command, Context);

            return operation.Complete<T>();
        }
    }
}