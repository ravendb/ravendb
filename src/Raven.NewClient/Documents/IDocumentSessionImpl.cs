//-----------------------------------------------------------------------
// <copyright file="IDocumentSessionImpl.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Document.Batches;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Documents
{
    /// <summary>
    /// Interface for document session which holds the internal operations
    /// </summary>
    internal interface IDocumentSessionImpl : IDocumentSession, ILazySessionOperations, IEagerSessionOperations
    {
        DocumentConvention Conventions { get; }

        T[] LoadInternal<T>(string[] ids);
        T[] LoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes);
        T[] LoadInternal<T>(string[] ids, string transformer, Dictionary<string, object> transformerParameters = null);
        T[] LoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, string transformer, Dictionary<string, object> transformerParameters = null);
        Lazy<T[]> LazyLoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, Action<T[]> onEval);
    }
}
