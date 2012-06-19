#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="RavenClientEnlistment.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Transactions;
using Raven.Abstractions.Logging;

namespace Raven.Client.Document
{
	/// <summary>
	/// An implementation of <see cref="IEnlistmentNotification"/> for the Raven Client API, allowing Raven
	/// Client API to participate in Distributed Transactions
	/// </summary>
	public class RavenClientEnlistment : IEnlistmentNotification
	{
		private static ILog logger = LogProvider.GetCurrentClassLogger();

		private readonly ITransactionalDocumentSession session;
		private readonly Action onTxComplete;
		private readonly TransactionInformation transaction;

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenClientEnlistment"/> class.
		/// </summary>
		public RavenClientEnlistment(ITransactionalDocumentSession session, Action onTxComplete)
		{
			transaction = Transaction.Current.TransactionInformation;
			this.session = session;
			this.onTxComplete = onTxComplete;
		}

		/// <summary>
		/// Notifies an enlisted object that a transaction is being prepared for commitment.
		/// </summary>
		/// <param name="preparingEnlistment">A <see cref="T:System.Transactions.PreparingEnlistment"/> object used to send a response to the transaction manager.</param>
		public void Prepare(PreparingEnlistment preparingEnlistment)
		{
			onTxComplete();
			try
			{
				session.StoreRecoveryInformation(session.ResourceManagerId, PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction), 
				                                 preparingEnlistment.RecoveryInformation());
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not prepare distributed transaction", e);
				preparingEnlistment.ForceRollback(e);
				return;
			}
			preparingEnlistment.Prepared();
		}

		/// <summary>
		/// Notifies an enlisted object that a transaction is being committed.
		/// </summary>
		/// <param name="enlistment">An <see cref="T:System.Transactions.Enlistment"/> object used to send a response to the transaction manager.</param>
		public void Commit(Enlistment enlistment)
		{
			onTxComplete();
			try
			{
				session.Commit(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not commit distributed transaction", e);
				return; // nothing to do, DTC will mark tx as hang
			}
			enlistment.Done();
		}

		/// <summary>
		/// Notifies an enlisted object that a transaction is being rolled back (aborted).
		/// </summary>
		/// <param name="enlistment">A <see cref="T:System.Transactions.Enlistment"/> object used to send a response to the transaction manager.</param>
		public void Rollback(Enlistment enlistment)
		{
			onTxComplete();
			try
			{
				session.Rollback(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not rollback distributed transaction", e);
			}
			enlistment.Done(); // will happen anyway, tx will be rolled back after timeout
		}

		/// <summary>
		/// Notifies an enlisted object that the status of a transaction is in doubt.
		/// </summary>
		/// <param name="enlistment">An <see cref="T:System.Transactions.Enlistment"/> object used to send a response to the transaction manager.</param>
		public void InDoubt(Enlistment enlistment)
		{
			onTxComplete();
			try
			{
				session.Rollback(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not mark distriubted transaction as in doubt", e);
			}
			enlistment.Done(); // what else can we do?
		}

		/// <summary>
		/// Initializes this instance.
		/// </summary>
		public void Initialize()
		{
		}

		/// <summary>
		/// Rollbacks the specified single phase enlistment.
		/// </summary>
		/// <param name="singlePhaseEnlistment">The single phase enlistment.</param>
		public void Rollback(SinglePhaseEnlistment singlePhaseEnlistment)
		{
			onTxComplete();
			try
			{
				session.Rollback(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not rollback distributed transaction", e);
				singlePhaseEnlistment.InDoubt(e);
				return;
			}
			singlePhaseEnlistment.Aborted();
		}
	}
}
#endif
