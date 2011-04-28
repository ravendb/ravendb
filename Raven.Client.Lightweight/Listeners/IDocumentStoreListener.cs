//-----------------------------------------------------------------------
// <copyright file="IDocumentStoreListener.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Json.Linq;

namespace Raven.Client.Listeners
{
    /// <summary>
    /// Hook for users to provide additional logic on store operations
    /// </summary>
    public interface IDocumentStoreListener
    {
        /// <summary>
        /// Invoked before the store request is sent to the server.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="entityInstance">The entity instance.</param>
        /// <param name="metadata">The metadata.</param>
        void BeforeStore(string key, object entityInstance, RavenJObject metadata);

        /// <summary>
        /// Invoked after the store request is sent to the server.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="entityInstance">The entity instance.</param>
        /// <param name="metadata">The metadata.</param>
        void AfterStore(string key, object entityInstance, RavenJObject metadata);
    }
}