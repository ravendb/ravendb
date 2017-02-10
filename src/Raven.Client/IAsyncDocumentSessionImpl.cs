//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Document.Batches;

namespace Raven.Client
{
    /// <summary>
    /// Interface for document session using async approaches
    /// </summary>
    internal interface IAsyncDocumentSessionImpl : IAsyncDocumentSession, IAsyncLazySessionOperations, IAsyncEagerSessionOperations
    {
        DocumentConventions Conventions { get; }

        Task<Dictionary<string, T>> LoadAsyncInternal<T>(string[] ids, string[] includes, CancellationToken token = default (CancellationToken));

        Task<Dictionary<string, T>> LoadUsingTransformerInternalAsync<T>(string[] ids, string[] includes, string transformer, Dictionary<string, object> transformerParameters = null, CancellationToken token = default (CancellationToken));

        Lazy<Task<Dictionary<string, T>>> LazyAsyncLoadInternal<T>(string[] ids, string[] includes, Action<Dictionary<string, T>> onEval, CancellationToken token = default (CancellationToken));

    }
}
