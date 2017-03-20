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
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced async session operations
    /// </summary>
    public partial interface IAsyncAdvancedSessionOperations
    {
        /// <summary>
        ///     DeleteByIndex using linq expression
        /// </summary>
        /// <param name="expression">The linq expression</param>
        /// <param name="patch"></param>
        /// <param name="options"></param>
        /// <param name="token"></param>
        Task<Operation> PatchByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression, PatchRequest patch, QueryOperationOptions options = null, CancellationToken token = default(CancellationToken)) where TIndexCreator : AbstractIndexCreationTask, new();

        /// <summary>
        ///     DeleteByIndex using linq expression
        /// </summary>
        /// <param name="indexName">Index name</param>
        /// <param name="expression">The linq expression</param>
        /// <param name="patch"></param>
        /// <param name="options"></param>
        /// <param name="token"></param>
        Task<Operation> PatchByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression, PatchRequest patch, QueryOperationOptions options = null, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     DeleteByIndexAsync using linq expression
        /// </summary>
        /// <param name="expression">The linq expression</param>
        /// <param name="options"></param>
        /// <param name="token"></param>
        Task<Operation> DeleteByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression, QueryOperationOptions options = null, CancellationToken token = default(CancellationToken)) where TIndexCreator : AbstractIndexCreationTask, new();

        /// <summary>
        ///     DeleteByIndexAsync using linq expression
        /// </summary>
        /// <param name="indexName">Index string name</param>
        /// <param name="expression">The linq expression</param>
        /// <param name="options"></param>
        /// <param name="token"></param>
        Task<Operation> DeleteByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression, QueryOperationOptions options = null, CancellationToken token = default(CancellationToken));
    }
}
