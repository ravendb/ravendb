//-----------------------------------------------------------------------
// <copyright file="IDocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
#if !SILVERLIGHT
using System.Collections.Specialized;
#endif
using System.Collections.Generic;
using System.Net;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
#else
using Raven.Client.Indexes;
#endif
#if !NET35
using Raven.Client.Connection.Async;
#endif

namespace Raven.Client
{
	/// <summary>
	/// Interface for managing access to RavenDB and open sessions.
	/// </summary>
	public interface IDocumentStore : IDisposalNotification
	{
		/// <summary>
		/// Setup the context for aggressive caching.
		/// </summary>
		/// <param name="cahceDuration">Specify the aggressive cache duration</param>
		/// <remarks>
		/// Aggressive caching means that we will not check the server to see whatever the response
		/// we provide is current or not, but will serve the information directly from the local cache
		/// without touching the server.
		/// </remarks>
		IDisposable AggressivelyCacheFor(TimeSpan cahceDuration);

		/// <summary>
		/// Setup the context for no aggressive caching
		/// </summary>
		/// <remarks>
		/// This is mainly useful for internal use inside RavenDB, when we are executing
		/// queries that has been marked with WaitForNonStaleResults, we temporarily disable
		/// aggressive caching.
		/// </remarks>
		IDisposable DisableAggressiveCaching();

		/// <summary>
		/// Gets the shared operations headers.
		/// </summary>
		/// <value>The shared operations headers.</value>
#if !SILVERLIGHT
		NameValueCollection SharedOperationsHeaders { get; }
#else
		IDictionary<string,string> SharedOperationsHeaders { get; }
#endif

		/// <summary>
		/// Get the <see cref="HttpJsonRequestFactory"/> for this store
		/// </summary>
		HttpJsonRequestFactory JsonRequestFactory { get; }

		/// <summary>
		/// Gets or sets the identifier for this store.
		/// </summary>
		/// <value>The identifier.</value>
		string Identifier { get; set; }

		/// <summary>
		/// Initializes this instance.
		/// </summary>
		/// <returns></returns>
		IDocumentStore Initialize();


#if !NET35
		/// <summary>
		/// Gets the async database commands.
		/// </summary>
		/// <value>The async database commands.</value>
		IAsyncDatabaseCommands AsyncDatabaseCommands { get; }

		/// <summary>
		/// Opens the async session.
		/// </summary>
		/// <returns></returns>
		IAsyncDocumentSession OpenAsyncSession();

		/// <summary>
		/// Opens the async session.
		/// </summary>
		/// <returns></returns>
		IAsyncDocumentSession OpenAsyncSession(string database);
#endif

#if !SILVERLIGHT
		/// <summary>
		/// Opens the session.
		/// </summary>
		/// <returns></returns>
		IDocumentSession OpenSession();

		/// <summary>
		/// Opens the session for a particular database
		/// </summary>
		IDocumentSession OpenSession(string database);

		/// <summary>
		/// Opens the session with the specified options.
		/// </summary>
		IDocumentSession OpenSession(OpenSessionOptions sessionOptions);

		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		IDatabaseCommands DatabaseCommands { get; }

		
		/// <summary>
		/// Executes the index creation.
		/// </summary>
		void ExecuteIndex(AbstractIndexCreationTask indexCreationTask);
#endif

		/// <summary>
		/// Gets the conventions.
		/// </summary>
		/// <value>The conventions.</value>
		DocumentConvention Conventions { get; }

		/// <summary>
		/// Gets the URL.
		/// </summary>
		string Url { get; }

		///<summary>
		/// Gets the etag of the last document written by any session belonging to this 
		/// document store
		///</summary>
		Guid? GetLastWrittenEtag();
	}
}