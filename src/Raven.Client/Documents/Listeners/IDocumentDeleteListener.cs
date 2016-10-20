//-----------------------------------------------------------------------
// <copyright file="IDocumentDeleteListener.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Json.Linq;
using Sparrow.Json;

namespace Raven.Client.Documents.Listeners
{
    /// <summary>
    /// Hook for users to provide additional logic on delete operations
    /// </summary>
    public interface IDocumentDeleteListener
    {
        /// <summary>
        /// Invoked before the delete request is sent to the server.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="entityInstance">The entity instance.</param>
        /// <param name="metadata">The metadata.</param>
        void BeforeDelete(string key, object entityInstance, BlittableJsonReaderObject metadata);
    }
}
