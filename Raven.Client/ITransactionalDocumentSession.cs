using System;

namespace Raven.Client
{
	/// <summary>
	/// Implementors of this interface provide transactional operations
	/// Note that this interface is mostly useful only for expert usage
	/// </summary>
	public interface ITransactionalDocumentSession
	{
        /// <summary>
        /// The transaction resource manager identifer
        /// </summary>
        Guid ResourceManagerId { get; }

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

	    /// <summary>
	    /// Stores the recovery information for the specified transaction
	    /// </summary>
	    /// <param name="resourceManagerId">The resource manager Id for this transaction</param>
	    /// <param name="txId">The tx id.</param>
	    /// <param name="recoveryInformation">The recovery information.</param>
	    void StoreRecoveryInformation(Guid resourceManagerId, Guid txId, byte[] recoveryInformation);
	}
}