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
using Raven.Client.Document;
#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
#endif
#if !NET_3_5
using Raven.Client.Connection.Async;
#endif

namespace Raven.Client
{
	/// <summary>
	/// Interface for managing access to RavenDB and open sessions.
	/// </summary>
	public interface IDocumentStore : IDisposable
	{
		/// <summary>
		/// Setup the context for aggresive caching.
		/// </summary>
		/// <param name="cahceDuration">Specify the aggresive cache duration</param>
		/// <remarks>
		/// Aggresive caching means that we will not check the server to see whatever the response
		/// we provide is current or not, but will serve the information directly from the local cache
		/// without touching the server.
		/// </remarks>
		IDisposable AggresivelyCacheFor(TimeSpan cahceDuration);

		/// <summary>
		/// Setup the context for no aggresive caching
		/// </summary>
		/// <remarks>
		/// This is mainly useful for internal use inside RavenDB, when we are executing
		/// queries that has been marked with WaitForNonStaleResults, we temporarily disable
		/// aggresive caching.
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
		/// Occurs when an entity is stored inside any session opened from this instance
		/// </summary>
		event EventHandler<StoredEntityEventArgs> Stored;

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


#if !NET_3_5
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
		/// Opens the session for a particular database with the specified credentials
		/// </summary>
		IDocumentSession OpenSession(string database, ICredentials credentialsForSession);

		/// <summary>
		/// Opens the session with the specified credentials.
		/// </summary>
		/// <param name="credentialsForSession">The credentials for session.</param>
		IDocumentSession OpenSession(ICredentials credentialsForSession);

		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
		IDatabaseCommands DatabaseCommands { get; }
#endif

		/// <summary>
		/// Gets the conventions.
		/// </summary>
		/// <value>The conventions.</value>
		DocumentConvention Conventions { get; }

		/// <summary>
		/// Gets or sets the URL.
		/// </summary>
		string Url { get; }
	}
}
