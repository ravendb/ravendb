//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Session
{
    /// <inheritdoc />
    public partial interface IAsyncDocumentSession
    {
        /// <summary>
        ///     Conditional load the specified entity with the specified id and changeVector.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be conditional loaded.</param>
        /// <param name="changeVector">Change vector of a entity that will be conditional loaded.</param>
        /// <param name="token">The cancellation token.</param>
        Task<(T Entity, string ChangeVector)> ConditionalLoadAsync<T>(string id, string changeVector, CancellationToken token = default(CancellationToken));
    }
}
