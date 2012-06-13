//-----------------------------------------------------------------------
// <copyright file="IDocumentSessionImpl.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client.Connection;
using Raven.Client.Document;
#if !NET35
using Raven.Client.Document.Batches;
#endif

namespace Raven.Client
{
	/// <summary>
	/// Interface for document session which holds the internal operations
	/// </summary>
	internal interface IDocumentSessionImpl : IDocumentSession
#if !NET35
	                                          , ILazySessionOperations, IEagerSessionOperations
#endif
	{
		DocumentConvention Conventions { get; }

		T[] LoadInternal<T>(string[] ids);
		T[] LoadInternal<T>(string[] ids, string[] includes);

#if !NET35
		Lazy<T[]> LazyLoadInternal<T>(string[] ids, string[] includes, Action<T[]> onEval);
#endif
	}
}