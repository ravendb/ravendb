//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced async session operations
    /// </summary>
    public partial interface IAsyncAdvancedSessionOperations
    {
        /// <summary>
        ///     Loads the specified entity with the specified id and changeVector.
        ///     If the entity is loaded into the session, the tracked entity will be returned otherwise the entity will be loaded only if it is fresher then the provided changeVector.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be conditional loaded.</param>
        /// <param name="changeVector">Change vector of a entity that will be conditional loaded.</param>
        /// <param name="token">The cancellation token.</param>
        Task<(T Entity, string ChangeVector)> ConditionalLoadAsync<T>(string id, string changeVector, CancellationToken token = default(CancellationToken));
    }
}
