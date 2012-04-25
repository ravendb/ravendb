//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET35

using System;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Indexes;

namespace Raven.Client
{
	using Linq;

	/// <summary>
	/// Interface for document session using async approaches
	/// </summary>
	public interface IAsyncDocumentSessionImpl : IAsyncDocumentSession
	{
		DocumentConvention Conventions { get; }

		Task<T[]> LoadAsyncInternal<T>(string[] ids, string[] includes);
	}
}
#endif