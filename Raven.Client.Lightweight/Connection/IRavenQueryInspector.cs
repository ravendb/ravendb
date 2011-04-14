//-----------------------------------------------------------------------
// <copyright file="IRavenQueryInspector.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
#if !NET_3_5
using Raven.Client.Connection.Async;
#endif

namespace Raven.Client.Connection
{

	/// <summary>
	/// Provide access to the underlying <see cref="IDocumentQuery{T}"/>
	/// </summary>
	internal interface IRavenQueryInspector
	{
		/// <summary>
		/// Get the name of the index being queried
		/// </summary>
		string IndexQueried { get; }

#if !SILVERLIGHT
		/// <summary>
		/// Grant access to the database commands
		/// </summary>
		IDatabaseCommands DatabaseCommands { get; }
#endif

#if !NET_3_5
		/// <summary>
		/// Grant access to the async database commands
		/// </summary>
		IAsyncDatabaseCommands AsyncDatabaseCommands { get; }
#endif

		/// <summary>
		/// The last term that we asked the query to use equals on
		/// </summary>
		KeyValuePair<string, string> GetLastEqualityTerm();
	}
}
