//-----------------------------------------------------------------------
// <copyright file="ITransactionalDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Client
{
	/// <summary>
	/// Implementers of this interface provide transactional operations
	/// Note that this interface is mostly useful only for expert usage
	/// </summary>
	public interface ITransactionalDocumentSession
	{
		/// <summary>
		/// The transaction resource manager identifier
		/// </summary>
		Guid ResourceManagerId { get; }

		/// <summary>
		/// The db name for this session
		/// </summary>
		string DatabaseName { get; }

		/// <summary>
		/// Commits the transaction specified.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		void Commit(Guid txId);

		/// <summary>
		/// Rollbacks the transaction specified.
		/// </summary>
		/// <param name="txId">The tx id.</param>
		void Rollback(Guid txId);

		/// <summary>
		/// Promotes a transaction specified to a distributed transaction
		/// </summary>
		/// <param name="fromTxId">From tx id.</param>
		/// <returns>The token representing the distributed transaction</returns>
		byte[] PromoteTransaction(Guid fromTxId);
	}
}
