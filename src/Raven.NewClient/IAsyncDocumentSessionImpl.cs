//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Client.Document;

using Raven.NewClient.Client.Document.Batches;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// Interface for document session using async approaches
    /// </summary>
    public interface IAsyncDocumentSessionImpl : IAsyncDocumentSession, IAsyncLazySessionOperations, IAsyncEagerSessionOperations
    {
        DocumentConvention Conventions { get; }

        Task<IDictionary<string, T>> LoadAsyncInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, CancellationToken token = default (CancellationToken));

        Task<IDictionary<string, T>> LoadUsingTransformerInternalAsync<T>(string[] ids, KeyValuePair<string, Type>[] includes, string transformer, Dictionary<string, object> transformerParameters = null, CancellationToken token = default (CancellationToken));

        Lazy<Task<IDictionary<string, T>>> LazyAsyncLoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, Action<IDictionary<string, T>> onEval, CancellationToken token = default (CancellationToken));

    }
}
