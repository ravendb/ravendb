//-----------------------------------------------------------------------
// <copyright file="IDocumentSessionImpl.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Document.Batches;


namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// Interface for document session which holds the internal operations
    /// </summary>
    internal interface IDocumentSessionImpl : IDocumentSession, ILazySessionOperations, IEagerSessionOperations
    {
        DocumentConvention Conventions { get; }

        Dictionary<string, T> LoadInternal<T>(string[] ids);
        Dictionary<string, T> LoadInternal<T>(string[] ids, string[] includes);
        T[] LoadInternal<T>(string[] ids, string transformer, Dictionary<string, object> transformerParameters = null);
        T[] LoadInternal<T>(string[] ids, string[] includes, string transformer, Dictionary<string, object> transformerParameters = null);
        Lazy<Dictionary<string, T>> LazyLoadInternal<T>(string[] ids, string[] includes, Action<Dictionary<string, T>> onEval);
    }
}
