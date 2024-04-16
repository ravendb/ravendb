//-----------------------------------------------------------------------
// <copyright file="IAsyncSessionDocumentCounters.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Session
{
    /// <inheritdoc cref="ISessionDocumentCounters"/>
    public interface IAsyncSessionDocumentCounters : ISessionDocumentCountersBase
    {
        /// <inheritdoc cref="ISessionDocumentCounters.GetAll"/>
        Task<Dictionary<string, long?>> GetAllAsync(CancellationToken token = default);

        /// <inheritdoc cref="ISessionDocumentCounters.Get(string)"/>
        Task<long?> GetAsync(string counter, CancellationToken token = default);

        /// <inheritdoc cref="ISessionDocumentCounters.Get(IEnumerable{string})"/> 
        Task<Dictionary<string, long?>> GetAsync(IEnumerable<string> counters, CancellationToken token = default);

    }
}
