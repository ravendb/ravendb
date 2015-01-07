//-----------------------------------------------------------------------
// <copyright file="ITransactionalDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Client
{
	/// <summary>
	///     Implementers of this interface provide transactional operations
	///     Note that this interface is mostly useful only for expert usage
	/// </summary>
	public interface ITransactionalDocumentSession
	{
		/// <summary>
		///     The database name for this session
		/// </summary>
		string DatabaseName { get; }
		/// <summary>
		///     The transaction resource manager identifier
		/// </summary>
		Guid ResourceManagerId { get; }

		/// <summary>
		///     Commits the specified tx id
		/// </summary>
		/// <param name="txId">transaction identifier</param>
		void Commit(string txId);

		/// <summary>
		///     Prepares the transaction on the server.
		/// </summary>
		void PrepareTransaction(string txId, Guid? resourceManagerId = null, byte[] recoveryInformation = null);

		/// <summary>
		///     Rollbacks the specified tx id
		/// </summary>
		/// <param name="txId">transaction identifier</param>
		void Rollback(string txId);
	}
}