//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;

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
        Operation DeleteByIndex<T, TIndexCreator>(Expression<Func<T, bool>> expression) where TIndexCreator : AbstractIndexCreationTask, new();

        /// <summary>
        ///     DeleteByIndex using linq expression
        /// </summary>
        /// <param name="indexName">Index string name</param>
        /// <param name="expression">The linq expression</param>
        Operation DeleteByIndex<T>(string indexName, Expression<Func<T, bool>> expression);
    }
}
