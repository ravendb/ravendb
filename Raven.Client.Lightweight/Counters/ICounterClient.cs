//-----------------------------------------------------------------------
// <copyright file="IAdvancedDocumentSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics.PerformanceData;
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
	public interface ICounterClient
	{
		/// <summary>
		/// The document store associated with this session
		/// </summary>
		/// <param name="group">The name of the counter to change.</param>
		/// <param name="counterName">The entity.</param>
		/// <param name="delta">The entity.</param>
		void Change(string group, string counterName, long delta);

		/// <summary>
		/// Returns whatever a document with the specified id is loaded in the 
		/// current session
		/// </summary>
		/// <param name="group">The name of the counter to change.</param>
		/// <param name="counterName">The name of the counter to change.</param>
		void Reset(string group, string counterName);

		/// <summary>
		/// Gets the store identifier for this session.
		/// The store identifier is the identifier for the particular RavenDB instance. 
		/// </summary>
		/// <param name="name">The name of the counter to change.</param>
		void Increment(string name);

		/// <summary>
		/// Evicts the specified entity from the session.
		/// Remove the entity from the delete queue and stops tracking changes for this entity.
		/// </summary>
		/// <param name="name">The name of the counter to change.</param>
		void Decrement(string name);

		/// <summary>
		/// Clears this instance.
		/// Remove all entities from the delete queue and stops tracking changes for all entities.
		/// </summary>
		/// <param name="name">The name of the counter to change.</param>
		/// <value>The store identifier.</value>
		long GetFinalValue(string name);

		/// <summary>
		/// Gets or sets a value indicating whether the session should use optimistic concurrency.
		/// When set to <c>true</c>, a check is made so that a change made behind the session back would fail
		/// and raise <see cref="ConcurrencyException"/>.
		/// </summary>
		/// <param name="name">The name of the counter to change.</param>
		/// <value>The store identifier.</value>
		CounterData GetServersValues(string name);

		/// <summary>
		/// Allow extensions to provide additional state per session
		/// </summary>
		/// <value>The store identifier.</value>
		ICounterBatch CreateBatch();
	}
}