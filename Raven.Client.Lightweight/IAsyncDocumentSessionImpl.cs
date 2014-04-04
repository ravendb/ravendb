//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Client.Document.Batches;

namespace Raven.Client
{
	/// <summary>
	/// Interface for document session using async approaches
	/// </summary>
    public interface IAsyncDocumentSessionImpl : IAsyncDocumentSession, IAsyncLazySessionOperations, IAsyncEagerSessionOperations
	{
		DocumentConvention Conventions { get; }

		Task<T[]> LoadAsyncInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes);

		Task<T[]> LoadAsyncInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, string transformer, Dictionary<string, RavenJToken> queryInputs = null);

       Lazy<Task<T[]>> LazyAsyncLoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, Action<T[]> onEval);

	}
}