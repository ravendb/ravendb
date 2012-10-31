//-----------------------------------------------------------------------
// <copyright file="IDocumentSessionImpl.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client.Document;
using Raven.Client.Document.Batches;

namespace Raven.Client
{
	/// <summary>
	/// Interface for document session which holds the internal operations
	/// </summary>
	internal interface IDocumentSessionImpl : IDocumentSession, ILazySessionOperations, IEagerSessionOperations
	{
		DocumentConvention Conventions { get; }

		T[] LoadInternal<T>(string[] ids);
		T[] LoadInternal<T>(string[] ids, string[] includes);
		
		Lazy<T[]> LazyLoadInternal<T>(string[] ids, string[] includes, Action<T[]> onEval);
	}
}