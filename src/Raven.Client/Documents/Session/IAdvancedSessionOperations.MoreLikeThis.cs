//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Raven.Client.Documents.Transformers;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperations
    {
        List<T> MoreLikeThis<T, TIndexCreator>(string documentId) where TIndexCreator : AbstractIndexCreationTask, new();

        List<T> MoreLikeThis<TTransformer, T, TIndexCreator>(string documentId, Parameters transformerParameters = null)
            where TIndexCreator : AbstractIndexCreationTask, new()
            where TTransformer : AbstractTransformerCreationTask, new();

        List<T> MoreLikeThis<TTransformer, T, TIndexCreator>(MoreLikeThisQuery query)
            where TIndexCreator : AbstractIndexCreationTask, new()
            where TTransformer : AbstractTransformerCreationTask, new();

        List<T> MoreLikeThis<T>(string index, string documentId, string transformer = null, Parameters transformerParameters = null);

        List<T> MoreLikeThis<T>(MoreLikeThisQuery query);
    }
}
