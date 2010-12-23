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
using Raven.Client.Client;
using Raven.Client.Document;

namespace Raven.Client
{
	/// <summary>
	/// Interface for managing access to RavenDB and open sessions.
	/// </summary>
    public interface IDocumentStore : IDisposable
    {
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

		/// <summary>
		/// Registers the delete listener.
		/// </summary>
		/// <param name="deleteListener">The delete listener.</param>
		/// <returns></returns>
    	IDocumentStore RegisterListener(IDocumentDeleteListener deleteListener);

		/// <summary>
		/// Registers the store listener.
		/// </summary>
		/// <param name="documentStoreListener">The document store listener.</param>
		/// <returns></returns>
		IDocumentStore RegisterListener(IDocumentStoreListener documentStoreListener);

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

		/// <summary>
		/// Gets the conventions.
		/// </summary>
		/// <value>The conventions.</value>
    	DocumentConvention Conventions { get; }
    }
}
