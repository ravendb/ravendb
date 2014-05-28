//-----------------------------------------------------------------------
// <copyright file="IAdvancedDocumentSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client.Exceptions;
using Raven.Json.Linq;

namespace Raven.Client.Counters
{
	/// <summary>
	/// Advanced session operations
	/// </summary>
	public interface ICounterBatch
	{
		/// <summary>
		/// The document store associated with this session
		/// </summary>
		void Change(string name, long delta);

		/// <summary>
		/// Returns whatever a document with the specified id is loaded in the 
		/// current session
		/// </summary>
		void Increment(string name);

		/// <summary>
		/// Gets the store identifier for this session.
		/// The store identifier is the identifier for the particular RavenDB instance. 
		/// </summary>
		/// <value>The store identifier.</value>
		void Decrement(string name);
	}
}