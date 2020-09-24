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
            if (string.IsNullOrEmpty(id))
                throw new ArgumentNullException(nameof(id));

            if (Advanced.IsLoaded(id))
            {
                var entity = Load<T>(id);
                if (entity == null)
                    return default;
                
                var cv = Advanced.GetChangeVectorFor(entity);
                return (entity, cv);
            }

            if (string.IsNullOrEmpty(changeVector))
                throw new InvalidOperationException($"The requested document with id '{id} is not loaded into the session and could not conditional load when {nameof(changeVector)} is null or empty.");

            IncrementRequestCount();
            var cmd = new ConditionalGetDocumentsCommand(id, changeVector);
            Advanced.RequestExecutor.Execute(cmd, Advanced.Context);

            switch (cmd.StatusCode)
            {
                case HttpStatusCode.NotModified:
                    return (default, changeVector); // value not changed
                case HttpStatusCode.NotFound:
                    RegisterMissing(id);
                    return default; // value is missing
            }

            var documentInfo = DocumentInfo.GetNewDocumentInfo((BlittableJsonReaderObject)cmd.Result.Results[0]);
            var r = TrackEntity<T>(documentInfo);

            return (r, cmd.Result.ChangeVector);
        }
    }
}
