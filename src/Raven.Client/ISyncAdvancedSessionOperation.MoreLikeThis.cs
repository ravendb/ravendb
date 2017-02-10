//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Indexes;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface ISyncAdvancedSessionOperation
    {
        List<T> MoreLikeThis<T, TIndexCreator>(string documentId) where TIndexCreator : AbstractIndexCreationTask, new();

        List<T> MoreLikeThis<T, TIndexCreator>(MoreLikeThisQuery query) where TIndexCreator : AbstractIndexCreationTask, new();

        List<T> MoreLikeThis<TTransformer, T, TIndexCreator>(string documentId, Dictionary<string, object> transformerParameters = null)
            where TIndexCreator : AbstractIndexCreationTask, new()
            where TTransformer : AbstractTransformerCreationTask, new();

        List<T> MoreLikeThis<TTransformer, T, TIndexCreator>(MoreLikeThisQuery query)
            where TIndexCreator : AbstractIndexCreationTask, new()
            where TTransformer : AbstractTransformerCreationTask, new();

        List<T> MoreLikeThis<T>(string index, string documentId, string transformer = null, Dictionary<string, object> transformerParameters = null);

        List<T> MoreLikeThis<T>(MoreLikeThisQuery query);
    }
}
