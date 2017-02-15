//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperation
    {
        /// <summary>
        ///     DeleteByIndex using linq expression
        /// </summary>
        /// <param name="expression">The linq expression</param>
        /// <param name="patch"></param>
        /// <param name="options"></param>
        Operation PatchByIndex<T, TIndexCreator>(Expression<Func<T, bool>> expression, PatchRequest patch, QueryOperationOptions options = null) where TIndexCreator : AbstractIndexCreationTask, new();

        /// <summary>
        ///     DeleteByIndex using linq expression
        /// </summary>
        /// <param name="indexName">Index name</param>
        /// <param name="expression">The linq expression</param>
        /// <param name="patch"></param>
        /// <param name="options"></param>
        Operation PatchByIndex<T>(string indexName, Expression<Func<T, bool>> expression, PatchRequest patch, QueryOperationOptions options = null);

        /// <summary>
        ///     DeleteByIndex using linq expression
        /// </summary>
        /// <param name="expression">The linq expression</param>
        /// <param name="options"></param>
        Operation DeleteByIndex<T, TIndexCreator>(Expression<Func<T, bool>> expression, QueryOperationOptions options = null) where TIndexCreator : AbstractIndexCreationTask, new();

        /// <summary>
        ///     DeleteByIndex using linq expression
        /// </summary>
        /// <param name="indexName">Index string name</param>
        /// <param name="expression">The linq expression</param>
        /// <param name="options"></param>
        Operation DeleteByIndex<T>(string indexName, Expression<Func<T, bool>> expression, QueryOperationOptions options = null);
    }
}
