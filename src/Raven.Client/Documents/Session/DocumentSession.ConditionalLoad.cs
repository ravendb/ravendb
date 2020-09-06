//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Net;
using Raven.Client.Documents.Commands;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        /// <inheritdoc />
        public (T Entity, string ChangeVector) ConditionalLoad<T>(string id, string changeVector)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (Advanced.IsLoaded(id))
            {
                var e = Load<T>(id);
                var cv = Advanced.GetChangeVectorFor(e);
                return (e, cv);
            }

            if (changeVector == null)
                return default;

            IncrementRequestCount();
            var cmd = new ConditionalGetDocumentsCommand(id, changeVector);
            Advanced.RequestExecutor.Execute(cmd, Advanced.Context);

            switch (cmd.StatusCode)
            {
                case HttpStatusCode.NotModified:
                    return (default, changeVector); // value not changed
                case HttpStatusCode.NotFound:
                    return default; // value is missing
            }

            if (cmd.Result.Results.Length == 0)
                return (default, cmd.Result.ChangeVector);

            var documentInfo = DocumentInfo.GetNewDocumentInfo((BlittableJsonReaderObject)cmd.Result.Results[0]);
            var r = TrackEntity<T>(documentInfo);

            return (r, cmd.Result.ChangeVector);
        }
    }
}
