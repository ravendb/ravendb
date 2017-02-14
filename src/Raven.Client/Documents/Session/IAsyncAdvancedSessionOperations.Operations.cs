//-----------------------------------------------------------------------
// <copyright file="IAsyncAdvancedSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced async session operations
    /// </summary>
    public partial interface IAsyncAdvancedSessionOperations
    {
        /// <summary>
        ///     DeleteByIndexAsync using linq expression
        /// </summary>
        /// <param name="expression">The linq expression</param>
        Task<Operation> DeleteByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression, CancellationToken token = default(CancellationToken)) where TIndexCreator : AbstractIndexCreationTask, new();

        /// <summary>
        ///     DeleteByIndexAsync using linq expression
        /// </summary>
        /// <param name="indexName">Index string name</param>
        /// <param name="expression">The linq expression</param>
        Task<Operation> DeleteByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression, CancellationToken token = default(CancellationToken));
    }
}
