//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.Operations.Lazy;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Interface for document session using async approaches
    /// </summary>
    internal interface IAsyncDocumentSessionImpl : IAsyncDocumentSession, IAsyncLazySessionOperations, IAsyncEagerSessionOperations
    {
        DocumentConventions Conventions { get; }

        Task<Dictionary<string, T>> LoadAsyncInternal<T>(string[] ids, string[] includes, string[] counterIncludes = null, bool includeAllCounters = false, IEnumerable<AbstractTimeSeriesRange> timeSeriesIncludes = null, string[] compareExchangeValueIncludes = null, CancellationToken token = default);

        Lazy<Task<Dictionary<string, T>>> LazyAsyncLoadInternal<T>(string[] ids, string[] includes, Action<Dictionary<string, T>> onEval, CancellationToken token = default);

    }
}
